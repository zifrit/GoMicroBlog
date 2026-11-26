//go:build integration

package repository_test

import (
	"context"
	"testing"
	"time"

	"MicroBlog/db/migrations"
	"MicroBlog/internal/database"
	"MicroBlog/internal/events"
	"MicroBlog/services/engagement/internal/repository"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestApplyPostCreatedCreatesZeroCounter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	repo, cleanup := newTestRepository(t, ctx)
	defer cleanup()

	event, err := events.NewPostCreated(42, "alice", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.ApplyEvent(ctx, event); err != nil {
		t.Fatal(err)
	}
	count, found, err := repo.PostLikeCount(ctx, 42)
	if err != nil || !found || count != 0 {
		t.Fatalf("counter after PostCreated: count=%d found=%t err=%v", count, found, err)
	}
}

func TestApplyPostLikedIncrementsCounter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	repo, cleanup := newTestRepository(t, ctx)
	defer cleanup()

	for _, userID := range []string{"alice", "bob"} {
		event, err := events.NewPostLiked(42, userID, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.ApplyEvent(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	count, found, err := repo.PostLikeCount(ctx, 42)
	if err != nil || !found || count != 2 {
		t.Fatalf("counter after two PostLiked events: count=%d found=%t err=%v", count, found, err)
	}
}

func TestApplyDuplicateEventDoesNotIncrement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	repo, cleanup := newTestRepository(t, ctx)
	defer cleanup()

	event, err := events.NewPostLiked(42, "alice", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := repo.ApplyEvent(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	count, found, err := repo.PostLikeCount(ctx, 42)
	if err != nil || !found || count != 1 {
		t.Fatalf("counter after duplicate event: count=%d found=%t err=%v", count, found, err)
	}
}

func TestPostLikedCreatesCounterForLegacyPost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	repo, cleanup := newTestRepository(t, ctx)
	defer cleanup()

	event, err := events.NewPostLiked(7, "alice", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.ApplyEvent(ctx, event); err != nil {
		t.Fatal(err)
	}
	count, found, err := repo.PostLikeCount(ctx, 7)
	if err != nil || !found || count != 1 {
		t.Fatalf("counter after first like for a legacy post: count=%d found=%t err=%v", count, found, err)
	}
}

func newTestRepository(t *testing.T, ctx context.Context) (*repository.Postgres, func()) {
	t.Helper()
	container, err := postgres.Run(ctx, "postgres:17.9-alpine", postgres.WithDatabase("microblog"),
		postgres.WithUsername("microblog"), postgres.WithPassword("test-password"), postgres.BasicWaitStrategies())
	testcontainers.CleanupContainer(t, container)
	if err != nil {
		t.Fatal(err)
	}
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := database.OpenPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := migrations.Up(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	return repository.New(pool), pool.Close
}
