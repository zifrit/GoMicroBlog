//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"MicroBlog/internal/models"
	"MicroBlog/internal/repository"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestApplicationPersistsAsyncLikesAcrossRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
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
	// prepareDatabase отменяет свой контекст перед возвратом.
	// Следующие HTTP-запросы проверяют, что готовый пул продолжает работать.
	pool, err := prepareDatabase(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var logs bytes.Buffer
	app := newApplication(&logs, repository.New(pool))
	appClosed := false
	defer func() {
		if !appClosed {
			app.Close()
		}
	}()
	call := func(handler http.Handler, method, path, body string, status int) []byte {
		t.Helper()
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(method, path, strings.NewReader(body)))
		if response.Code != status {
			t.Fatalf("%s %s: %d, %s", method, path, response.Code, response.Body.String())
		}
		return response.Body.Bytes()
	}
	call(app.Handler, "POST", "/register", `{"username":" alice "}`, http.StatusCreated)
	call(app.Handler, "POST", "/register", `{"username":"alice"}`, http.StatusBadRequest)
	call(app.Handler, "POST", "/posts", `{"username":"missing","text":"hello"}`, http.StatusNotFound)
	body := call(app.Handler, "POST", "/posts", `{"username":"alice","text":" hello "}`, http.StatusCreated)
	var created models.Post
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	// Отклонённый выше INSERT может занять значение identity, поэтому номера не обязаны идти подряд.
	expected := fmt.Sprintf(`{"id":%d,"author":{"id":"alice","username":"alice"},"text":"hello","likes":[]}`, created.ID)
	if created.ID <= 0 || string(bytes.TrimSpace(body)) != expected {
		t.Fatalf("post JSON: %s", body)
	}
	likePath := fmt.Sprintf("/posts/%d/like", created.ID)
	call(app.Handler, "POST", likePath, `{"username":"missing"}`, http.StatusNotFound)
	call(app.Handler, "POST", "/posts/999/like", `{"username":"alice"}`, http.StatusNotFound)
	call(app.Handler, "POST", likePath, `{"username":" alice "}`, http.StatusAccepted)
	// При закрытии обрабатываем принятую задачу до закрытия пула.
	app.Close()
	appClosed = true
	pool.Close()
	reopened, err := repository.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restarted := newApplication(&bytes.Buffer{}, repository.New(reopened))
	defer restarted.Close()
	body = call(restarted.Handler, "GET", "/posts", "", http.StatusOK)
	var posts []*models.Post
	if err := json.Unmarshal(body, &posts); err != nil {
		t.Fatal(err)
	}
	if len(posts) != 1 || posts[0].Text != "hello" || len(posts[0].Likes) != 1 || posts[0].Likes[0] != "alice" {
		t.Fatalf("persisted posts: %s", body)
	}
	if !strings.Contains(logs.String(), "post liked: alice") {
		t.Fatalf("missing committed event: %s", logs.String())
	}
	reopened.Close()
	call(restarted.Handler, "GET", "/posts", "", http.StatusInternalServerError)
}
