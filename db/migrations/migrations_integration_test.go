//go:build integration

package migrations_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"MicroBlog/db/migrations"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestSprint4MigrationPreservesRowsAndPartitions(t *testing.T) {
	ctx, pool := startSprint4Postgres(t)
	if err := migrations.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Down(ctx, pool); err != nil {
		t.Fatalf("return to schema version 1 before seeding legacy rows: %v", err)
	}

	const createdAt = "2025-03-04T12:00:00Z"
	var postID int64
	if _, err := pool.Exec(ctx, `INSERT INTO public.users (id, username) VALUES ('alice', 'alice')`); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO public.posts (user_id, text, created_at)
		VALUES ('alice', 'preserved post', $1)
		RETURNING id`, createdAt).Scan(&postID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.likes (post_id, post_created_at, user_id)
		VALUES ($1, $2, 'alice')`, postID, createdAt); err != nil {
		t.Fatal(err)
	}

	if err := migrations.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var newTableCount int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE (n.nspname, c.relname) IN (
			('api', 'outbox_events'),
			('engagement', 'processed_events'),
			('engagement', 'post_like_counters')
		)`).Scan(&newTableCount); err != nil || newTableCount != 3 {
		t.Fatalf("sprint 4 tables: count %d, %v", newTableCount, err)
	}
	var username, postText, liker string
	if err := pool.QueryRow(ctx, `SELECT username FROM api.users WHERE id = 'alice'`).Scan(&username); err != nil || username != "alice" {
		t.Fatalf("preserved user: %q, %v", username, err)
	}
	if err := pool.QueryRow(ctx, `SELECT text FROM api.posts WHERE id = $1`, postID).Scan(&postText); err != nil || postText != "preserved post" {
		t.Fatalf("preserved post: %q, %v", postText, err)
	}
	if err := pool.QueryRow(ctx, `SELECT user_id FROM api.likes WHERE post_id = $1`, postID).Scan(&liker); err != nil || liker != "alice" {
		t.Fatalf("preserved like: %q, %v", liker, err)
	}

	partitions, err := listPostPartitions(ctx, pool, "api")
	if err != nil {
		t.Fatal(err)
	}
	wantPartitions := []string{"posts_2024", "posts_2025", "posts_2026", "posts_2027", "posts_default"}
	if !reflect.DeepEqual(partitions, wantPartitions) {
		t.Fatalf("post partitions: got %v, want %v", partitions, wantPartitions)
	}

	var indexCount int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_indexes
		WHERE schemaname = 'api'
		  AND indexname IN ('posts_user_id_idx', 'likes_user_id_idx', 'likes_pkey')`).Scan(&indexCount); err != nil || indexCount != 3 {
		t.Fatalf("preserved indexes: count %d, %v", indexCount, err)
	}
	var foreignKeyCount int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_constraint
		WHERE conrelid = 'api.likes'::regclass
		  AND confrelid IN ('api.users'::regclass, 'api.posts'::regclass)`).Scan(&foreignKeyCount); err != nil || foreignKeyCount != 2 {
		t.Fatalf("preserved foreign keys: count %d, %v", foreignKeyCount, err)
	}
	var generatedPostID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO api.posts (user_id, text) VALUES ('alice', 'identity after move') RETURNING id`).Scan(&generatedPostID); err != nil || generatedPostID <= postID {
		t.Fatalf("identity after move: got %d after %d, %v", generatedPostID, postID, err)
	}

	if err := migrations.Down(ctx, pool); err != nil {
		t.Fatalf("down migration should return existing data to public: %v", err)
	}
	var returnedText string
	if err := pool.QueryRow(ctx, `SELECT text FROM public.posts WHERE id = $1`, postID).Scan(&returnedText); err != nil || returnedText != "preserved post" {
		t.Fatalf("row after down: %q, %v", returnedText, err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO public.posts (user_id, text) VALUES ('alice', 'identity after down') RETURNING id`).Scan(&generatedPostID); err != nil || generatedPostID <= postID {
		t.Fatalf("identity after down: got %d after %d, %v", generatedPostID, postID, err)
	}
}

func TestSprint4DownRejectsPendingOutbox(t *testing.T) {
	ctx, pool := startSprint4Postgres(t)
	if err := migrations.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Down(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	eventID := uuid.New()
	if _, err := pool.Exec(ctx, `
		INSERT INTO api.outbox_events (event_id, event_type, schema_version, message_key, payload, occurred_at)
		VALUES ($1, 'PostCreated', 1, 'post:1', '{"post_id":1,"author_id":"alice"}', now())`, eventID); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Down(ctx, pool); err == nil {
		t.Fatal("down migration succeeded with an unpublished outbox event")
	}
	var remaining int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM api.outbox_events WHERE event_id = $1`, eventID).Scan(&remaining); err != nil || remaining != 1 {
		t.Fatalf("pending event after rejected down: count %d, %v", remaining, err)
	}
}

func TestSprint4DownWaitsForConcurrentOutboxInsert(t *testing.T) {
	ctx, pool := startSprint4Postgres(t)
	if err := migrations.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	eventID := uuid.New()
	if _, err := tx.Exec(ctx, `
		INSERT INTO api.outbox_events (event_id, event_type, schema_version, message_key, payload, occurred_at)
		VALUES ($1, 'PostCreated', 1, 'post:1', '{"post_id":1,"author_id":"alice"}', now())`, eventID); err != nil {
		t.Fatal(err)
	}

	downContext, cancelDown := context.WithTimeout(ctx, 15*time.Second)
	defer cancelDown()
	downDone := make(chan error, 1)
	go func() {
		downDone <- migrations.Down(downContext, pool)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting int
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM pg_stat_activity
			WHERE datname = current_database()
			  AND pid <> pg_backend_pid()
			  AND state = 'active'
			  AND wait_event_type = 'Lock'`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Down migration did not wait for the uncommitted outbox insert")
		}
		time.Sleep(25 * time.Millisecond)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-downDone; err == nil {
		t.Fatal("down migration succeeded after the concurrent pending event committed")
	}
	var remaining int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM api.outbox_events WHERE event_id = $1`, eventID).Scan(&remaining); err != nil || remaining != 1 {
		t.Fatalf("concurrent pending event after rejected down: count %d, %v", remaining, err)
	}
}

func startSprint4Postgres(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	container, err := postgres.Run(ctx, "postgres:17.9-alpine",
		postgres.WithDatabase("microblog"), postgres.WithUsername("microblog"),
		postgres.WithPassword("test-password"), postgres.BasicWaitStrategies())
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, container)
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return ctx, pool
}

func listPostPartitions(ctx context.Context, pool *pgxpool.Pool, schema string) ([]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT child.relname
		FROM pg_inherits i
		JOIN pg_class parent ON parent.oid = i.inhparent
		JOIN pg_namespace parent_schema ON parent_schema.oid = parent.relnamespace
		JOIN pg_class child ON child.oid = i.inhrelid
		WHERE parent_schema.nspname = $1 AND parent.relname = 'posts'
		ORDER BY child.relname`, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	partitions := make([]string, 0, 5)
	for rows.Next() {
		var partition string
		if err := rows.Scan(&partition); err != nil {
			return nil, fmt.Errorf("read partition: %w", err)
		}
		partitions = append(partitions, partition)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return partitions, nil
}
