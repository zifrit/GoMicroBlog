//go:build integration

package repository_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"MicroBlog/db/migrations"
	"MicroBlog/internal/repository"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestPostgresPersistenceAndConstraints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	container, err := postgres.Run(ctx, "postgres:17.9-alpine",
		postgres.WithDatabase("microblog"), postgres.WithUsername("microblog"),
		postgres.WithPassword("test-password"), postgres.BasicWaitStrategies())
	testcontainers.CleanupContainer(t, container)
	if err != nil {
		t.Fatal(err)
	}
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := repository.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { pool.Close() }()
	if err := migrations.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	// Повторный запуск не применяет миграцию, которая уже выполнена.
	if err := migrations.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	db := repository.New(pool)
	posts, err := db.ListPosts(ctx)
	if err != nil || posts == nil || len(posts) != 0 {
		t.Fatalf("empty feed: %v, %v", posts, err)
	}
	user, err := db.Register(ctx, "alice")
	if err != nil || user.ID != "alice" {
		t.Fatalf("register: %v, %v", user, err)
	}
	if _, err := db.Register(ctx, "alice"); !errors.Is(err, repository.ErrUserExists) {
		t.Fatalf("duplicate user: %v", err)
	}
	if _, err := db.CreatePost(ctx, "missing", "hello"); !errors.Is(err, repository.ErrUserNotFound) {
		t.Fatalf("unknown author: %v", err)
	}
	post, err := db.CreatePost(ctx, "alice", "hello")
	if err != nil {
		t.Fatal(err)
	}
	if post.Likes == nil {
		t.Fatal("likes must be []")
	}
	if err := db.ValidateLike(ctx, post.ID, "missing"); !errors.Is(err, repository.ErrUserNotFound) {
		t.Fatalf("unknown liker: %v", err)
	}
	if err := db.ValidateLike(ctx, -1, "alice"); !errors.Is(err, repository.ErrPostNotFound) {
		t.Fatalf("unknown post: %v", err)
	}
	if _, err := db.LikePost(ctx, -1, "alice"); !errors.Is(err, repository.ErrPostNotFound) {
		t.Fatalf("like missing post: %v", err)
	}
	if _, err := db.LikePost(ctx, post.ID, "missing"); !errors.Is(err, repository.ErrUserNotFound) {
		t.Fatalf("like missing user: %v", err)
	}

	t.Run("one concurrent like wins", func(t *testing.T) {
		var wins atomic.Int32
		var wg sync.WaitGroup
		for range 12 {
			wg.Go(func() {
				_, err := db.LikePost(ctx, post.ID, "alice")
				if err == nil {
					wins.Add(1)
				} else if !errors.Is(err, repository.ErrAlreadyLiked) {
					t.Errorf("like: %v", err)
				}
			})
		}
		wg.Wait()
		if wins.Load() != 1 {
			t.Fatalf("successful likes: %d", wins.Load())
		}
	})
	t.Run("one concurrent registration wins", func(t *testing.T) {
		var wins atomic.Int32
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				_, err := db.Register(ctx, "bob")
				if err == nil {
					wins.Add(1)
				} else if !errors.Is(err, repository.ErrUserExists) {
					t.Errorf("register: %v", err)
				}
			})
		}
		wg.Wait()
		if wins.Load() != 1 {
			t.Fatalf("successful registrations: %d", wins.Load())
		}
	})
	t.Run("commit failure rolls back inserted like", func(t *testing.T) {
		_, err := pool.Exec(ctx, `CREATE FUNCTION reject_bob_like() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.user_id = 'bob' THEN RAISE EXCEPTION 'simulated commit failure'; END IF;
                RETURN NEW;
            END $$;
            CREATE CONSTRAINT TRIGGER reject_bob_like AFTER INSERT ON likes
            DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_bob_like()`)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.LikePost(ctx, post.ID, "bob"); err == nil {
			t.Fatal("commit should fail")
		}
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM likes WHERE user_id='bob'`).Scan(&count); err != nil || count != 0 {
			t.Fatalf("rollback: %d likes, %v", count, err)
		}
		if _, err := pool.Exec(ctx, `DROP TRIGGER reject_bob_like ON likes; DROP FUNCTION reject_bob_like()`); err != nil {
			t.Fatal(err)
		}
	})

	// После отката дублирующего лайка пул должен оставаться рабочим, лишних строк быть не должно.
	posts, err = db.ListPosts(ctx)
	if err != nil || len(posts) != 1 || len(posts[0].Likes) != 1 || posts[0].Likes[0] != "alice" {
		t.Fatalf("feed: %+v, %v", posts, err)
	}

	pool.Close()
	pool, err = repository.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	db = repository.New(pool)
	posts, err = db.ListPosts(ctx)
	if err != nil || len(posts) != 1 || posts[0].Text != "hello" || len(posts[0].Likes) != 1 {
		t.Fatalf("persisted feed: %+v, %v", posts, err)
	}
	next, err := db.CreatePost(ctx, "alice", "after reconnect")
	if err != nil || next.ID <= post.ID {
		t.Fatalf("persistent identity: %+v, %v", next, err)
	}

	t.Run("feed groups likes and preserves posts without likes", func(t *testing.T) {
		likedPost, err := db.LikePost(ctx, post.ID, "bob")
		if err != nil {
			t.Fatal(err)
		}
		if len(likedPost.Likes) != 2 || likedPost.Likes[0] != "alice" || likedPost.Likes[1] != "bob" {
			t.Fatalf("liked post: %+v", likedPost)
		}
		feed, err := db.ListPosts(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(feed) != 2 {
			t.Fatalf("expected two posts, got %d", len(feed))
		}
		if feed[0].ID != post.ID || feed[0].Text != "hello" || len(feed[0].Likes) != 2 {
			t.Fatalf("first post: %+v", feed[0])
		}
		if feed[1].ID != next.ID || feed[1].Likes == nil || len(feed[1].Likes) != 0 {
			t.Fatalf("post without likes: %+v", feed[1])
		}
		if feed[0].Author.ID != "alice" || feed[0].Author.Username != "alice" {
			t.Fatalf("author: %+v", feed[0].Author)
		}
	})

	t.Run("year partitions and default", func(t *testing.T) {
		for _, tc := range []struct {
			year      int
			partition string
		}{{2024, "posts_2024"}, {2025, "posts_2025"}, {2026, "posts_2026"}, {2027, "posts_2027"}, {2035, "posts_default"}, {2023, "posts_default"}} {
			var table string
			err := pool.QueryRow(ctx, `INSERT INTO posts (user_id, text, created_at) VALUES ('alice', 'dated', $1) RETURNING tableoid::regclass::text`, time.Date(tc.year, 1, 1, 0, 0, 0, 0, time.UTC)).Scan(&table)
			if err != nil || table != tc.partition {
				t.Errorf("year %d: %s, %v", tc.year, table, err)
			}
		}
	})
	t.Run("database rejects invalid data", func(t *testing.T) {
		for _, sql := range []string{
			`INSERT INTO users (id, username) VALUES ('', '')`,
			`INSERT INTO posts (user_id, text) VALUES ('alice', '  ')`,
			`INSERT INTO posts (user_id, text) VALUES ('missing', 'hello')`,
			`INSERT INTO likes (post_id, post_created_at, user_id) VALUES (-1, now(), 'alice')`,
			fmt.Sprintf(`INSERT INTO likes (post_id, post_created_at, user_id) SELECT id, created_at, 'missing' FROM posts WHERE id = %d`, post.ID),
		} {
			if _, err := pool.Exec(ctx, sql); err == nil {
				t.Errorf("accepted invalid data: %s", sql)
			}
		}
	})
	t.Run("canceled request", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := db.ListPosts(canceled); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled query: %v", err)
		}
	})
	t.Run("indexes and explain", func(t *testing.T) {
		for _, name := range []string{"posts_user_id_idx", "likes_user_id_idx", "likes_pkey"} {
			var count int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_indexes WHERE indexname=$1 AND indexdef LIKE '%USING btree%'`, name).Scan(&count); err != nil || count != 1 {
				t.Fatalf("index %s: %d, %v", name, count, err)
			}
		}
		rows, err := pool.Query(ctx, `EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM posts WHERE user_id='alice' AND created_at >= '2026-01-01' AND created_at < '2027-01-01'`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			t.Log(line)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("migration down and up", func(t *testing.T) {
		if err := migrations.Down(ctx, pool); err != nil {
			t.Fatal(err)
		}
		var table *string
		if err := pool.QueryRow(ctx, `SELECT to_regclass('public.posts')::text`).Scan(&table); err != nil || table != nil {
			t.Fatalf("down: %v, %v", table, err)
		}
		if err := migrations.Up(ctx, pool); err != nil {
			t.Fatal(err)
		}
		posts, err := repository.New(pool).ListPosts(ctx)
		if err != nil || len(posts) != 0 {
			t.Fatalf("fresh feed: %v, %v", posts, err)
		}
	})
}
