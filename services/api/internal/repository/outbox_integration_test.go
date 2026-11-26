//go:build integration

package repository_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"MicroBlog/db/migrations"
	"MicroBlog/internal/database"
	"MicroBlog/services/api/internal/repository"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestRegisterCreatesOutboxEventAtomically(t *testing.T) {
	ctx, pool, db := newOutboxTestDB(t)
	user, err := db.Register(ctx, "alice")
	if err != nil || user.ID != "alice" {
		t.Fatalf("register: %+v, %v", user, err)
	}
	assertOutboxEvent(t, ctx, pool, "UserRegistered", "user:alice", map[string]any{"user_id": "alice"})

	if _, err := pool.Exec(ctx, `
		CREATE FUNCTION api.reject_bob_registration_event() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.message_key = 'user:bob' THEN RAISE EXCEPTION 'simulated outbox failure'; END IF;
			RETURN NEW;
		END $$;
		CREATE TRIGGER reject_bob_registration_event BEFORE INSERT ON api.outbox_events
		FOR EACH ROW EXECUTE FUNCTION api.reject_bob_registration_event()`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Register(ctx, "bob"); err == nil {
		t.Fatal("registration succeeded after its outbox insert failed")
	}
	var bobCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM api.users WHERE id = 'bob'`).Scan(&bobCount); err != nil || bobCount != 0 {
		t.Fatalf("user row after failed outbox insert: count %d, %v", bobCount, err)
	}
	if _, err := pool.Exec(ctx, `DROP TRIGGER reject_bob_registration_event ON api.outbox_events; DROP FUNCTION api.reject_bob_registration_event()`); err != nil {
		t.Fatal(err)
	}
}

func TestCreatePostCreatesOutboxEventAtomically(t *testing.T) {
	ctx, pool, db := newOutboxTestDB(t)
	if _, err := db.Register(ctx, "alice"); err != nil {
		t.Fatal(err)
	}
	post, err := db.CreatePost(ctx, "alice", "hello")
	if err != nil {
		t.Fatal(err)
	}
	assertOutboxEvent(t, ctx, pool, "PostCreated", fmt.Sprintf("post:%d", post.ID), map[string]any{
		"post_id": float64(post.ID), "author_id": "alice",
	})
}

func TestLikeCreatesOneOutboxEventAtomically(t *testing.T) {
	ctx, pool, db := newOutboxTestDB(t)
	if _, err := db.Register(ctx, "alice"); err != nil {
		t.Fatal(err)
	}
	post, err := db.CreatePost(ctx, "alice", "hello")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.LikePost(ctx, post.ID, "alice"); err != nil {
		t.Fatal(err)
	}
	assertOutboxEvent(t, ctx, pool, "PostLiked", fmt.Sprintf("post:%d", post.ID), map[string]any{
		"post_id": float64(post.ID), "user_id": "alice",
	})
	var likeCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM api.likes WHERE post_id = $1`, post.ID).Scan(&likeCount); err != nil || likeCount != 1 {
		t.Fatalf("likes: count %d, %v", likeCount, err)
	}
}

func TestConcurrentDuplicateLikeCreatesOneOutboxEvent(t *testing.T) {
	ctx, pool, db := newOutboxTestDB(t)
	if _, err := db.Register(ctx, "alice"); err != nil {
		t.Fatal(err)
	}
	post, err := db.CreatePost(ctx, "alice", "hello")
	if err != nil {
		t.Fatal(err)
	}

	const requests = 12
	var group sync.WaitGroup
	var successfulLikes int
	var alreadyLiked int
	var resultMu sync.Mutex
	for range requests {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := db.LikePost(ctx, post.ID, "alice")
			resultMu.Lock()
			defer resultMu.Unlock()
			switch {
			case err == nil:
				successfulLikes++
			case errors.Is(err, repository.ErrAlreadyLiked):
				alreadyLiked++
			default:
				t.Errorf("like: %v", err)
			}
		}()
	}
	group.Wait()
	if successfulLikes != 1 || alreadyLiked != requests-1 {
		t.Fatalf("successful=%d already-liked=%d", successfulLikes, alreadyLiked)
	}
	if count := countOutboxEvents(t, ctx, pool, "PostLiked"); count != 1 {
		t.Fatalf("PostLiked outbox events: got %d, want 1", count)
	}
	var likeCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM api.likes WHERE post_id = $1`, post.ID).Scan(&likeCount); err != nil || likeCount != 1 {
		t.Fatalf("likes: count %d, %v", likeCount, err)
	}
}

func newOutboxTestDB(t *testing.T) (context.Context, *pgxpool.Pool, *repository.Postgres) {
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
	pool, err := database.OpenPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := migrations.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return ctx, pool, repository.New(pool)
}

func assertOutboxEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventType, messageKey string, expectedPayload map[string]any) {
	t.Helper()
	var eventID string
	var gotType, gotKey string
	var schemaVersion int
	var payloadBytes []byte
	var occurredAt time.Time
	if err := pool.QueryRow(ctx, `
		SELECT event_id::text, event_type, schema_version, message_key, payload, occurred_at
		FROM api.outbox_events WHERE event_type = $1 ORDER BY id LIMIT 1`, eventType).
		Scan(&eventID, &gotType, &schemaVersion, &gotKey, &payloadBytes, &occurredAt); err != nil {
		t.Fatalf("read %s outbox event: %v", eventType, err)
	}
	if eventID == "" || gotType != eventType || schemaVersion != 1 || gotKey != messageKey || occurredAt.IsZero() {
		t.Fatalf("outbox metadata: id=%q type=%q version=%d key=%q occurred_at=%s", eventID, gotType, schemaVersion, gotKey, occurredAt)
	}
	var gotPayload map[string]any
	if err := json.Unmarshal(payloadBytes, &gotPayload); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotPayload, expectedPayload) {
		t.Fatalf("outbox payload: got %#v, want %#v", gotPayload, expectedPayload)
	}
	if count := countOutboxEvents(t, ctx, pool, eventType); count != 1 {
		t.Fatalf("%s outbox event count: got %d, want 1", eventType, count)
	}
}

func countOutboxEvents(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventType string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM api.outbox_events WHERE event_type = $1`, eventType).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
