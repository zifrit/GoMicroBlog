//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"MicroBlog/internal/database"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestAPIServesWritesWhileKafkaUnavailable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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
	address := availableAddress(t)
	closedBroker := availableAddress(t)
	t.Setenv("DATABASE_URL", dsn)
	t.Setenv("KAFKA_BROKERS", closedBroker)
	t.Setenv("HTTP_ADDR", address)

	runCtx, stop := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- run(runCtx) }()
	defer func() {
		stop()
		select {
		case err := <-runDone:
			if err != nil {
				t.Errorf("API shutdown: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("API did not stop")
		}
	}()

	baseURL := "http://" + address
	client := &http.Client{Timeout: time.Second}
	if err := waitForAPI(ctx, func() (bool, error) {
		response, err := client.Get(baseURL + "/healthz")
		if err != nil {
			return false, nil
		}
		defer response.Body.Close()
		return response.StatusCode == http.StatusOK, nil
	}); err != nil {
		t.Fatalf("API did not become ready with Kafka unavailable: %v", err)
	}
	post := func(path, body string, wantStatus int) string {
		t.Helper()
		response, err := client.Post(baseURL+path, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != wantStatus {
			t.Fatalf("POST %s: status %d, want %d", path, response.StatusCode, wantStatus)
		}
		result, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		return string(result)
	}
	post("/register", `{"username":"alice"}`, http.StatusCreated)
	created := post("/posts", `{"username":"alice","text":"hello"}`, http.StatusCreated)
	var postResponse struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal([]byte(created), &postResponse); err != nil {
		t.Fatal(err)
	}
	post(fmt.Sprintf("/posts/%d/like", postResponse.ID), `{"username":"alice"}`, http.StatusAccepted)

	pool, err := database.OpenPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var pending int
	if err := waitForAPI(ctx, func() (bool, error) {
		err := pool.QueryRow(ctx, `SELECT count(*) FROM api.outbox_events WHERE published_at IS NULL`).Scan(&pending)
		return pending == 3, err
	}); err != nil {
		t.Fatalf("expected three durable outbox events while Kafka is unavailable: pending=%d, err=%v", pending, err)
	}
}

func availableAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func waitForAPI(ctx context.Context, check func() (bool, error)) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready, err := check()
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
