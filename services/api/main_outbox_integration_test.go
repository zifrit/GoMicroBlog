//go:build integration

package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"MicroBlog/services/api/internal/repository"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestLikeCommitsBeforeReturning202(t *testing.T) {
	ctx, pool, app := newAPIOutboxTestApp(t)
	db := repository.New(pool)
	if _, err := db.Register(ctx, "alice"); err != nil {
		t.Fatal(err)
	}
	post, err := db.CreatePost(ctx, "alice", "hello")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE FUNCTION api.delay_like_insert() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM pg_sleep(2);
			RETURN NEW;
		END $$;
		CREATE TRIGGER delay_like_insert BEFORE INSERT ON api.likes
		FOR EACH ROW EXECUTE FUNCTION api.delay_like_insert()`); err != nil {
		t.Fatal(err)
	}

	response := httptest.NewRecorder()
	app.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		fmt.Sprintf("/posts/%d/like", post.ID), strings.NewReader(`{"username":"alice"}`)))
	if response.Code != http.StatusAccepted {
		t.Fatalf("like status: got %d, body %s", response.Code, response.Body.String())
	}
	var likes, events int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM api.likes WHERE post_id = $1 AND user_id = 'alice'`, post.ID).Scan(&likes); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM api.outbox_events WHERE event_type = 'PostLiked'`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if likes != 1 || events != 1 {
		t.Fatalf("after 202 response: likes=%d PostLiked events=%d", likes, events)
	}
}

func TestDuplicateLikeReturns202WithoutSecondEvent(t *testing.T) {
	ctx, pool, app := newAPIOutboxTestApp(t)
	db := repository.New(pool)
	if _, err := db.Register(ctx, "alice"); err != nil {
		t.Fatal(err)
	}
	post, err := db.CreatePost(ctx, "alice", "hello")
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/posts/%d/like", post.ID), strings.NewReader(`{"username":"alice"}`))
	first := httptest.NewRecorder()
	app.Handler.ServeHTTP(first, request)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first like status: got %d, body %s", first.Code, first.Body.String())
	}
	second := httptest.NewRecorder()
	app.Handler.ServeHTTP(second, httptest.NewRequest(http.MethodPost,
		fmt.Sprintf("/posts/%d/like", post.ID), strings.NewReader(`{"username":"alice"}`)))
	if second.Code != http.StatusAccepted || !bytes.Equal(first.Body.Bytes(), second.Body.Bytes()) {
		t.Fatalf("duplicate like response: first=%d %s second=%d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
	}
	var likes, events int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM api.likes WHERE post_id = $1 AND user_id = 'alice'`, post.ID).Scan(&likes); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM api.outbox_events WHERE event_type = 'PostLiked'`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if likes != 1 || events != 1 {
		t.Fatalf("after duplicate request: likes=%d PostLiked events=%d", likes, events)
	}
}

func newAPIOutboxTestApp(t *testing.T) (context.Context, *pgxpool.Pool, *application) {
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
	pool, err := prepareDatabase(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	app := newApplication(repository.New(pool))
	return ctx, pool, app
}
