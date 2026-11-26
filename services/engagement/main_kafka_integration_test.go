//go:build integration

package main

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"MicroBlog/db/migrations"
	"MicroBlog/internal/database"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestEngagementServesRESTWhileKafkaUnavailable(t *testing.T) {
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
	pool, err := database.OpenPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := migrations.Up(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	pool.Close()

	address := availableHTTPAddress(t)
	closedBroker := availableHTTPAddress(t)
	t.Setenv("DATABASE_URL", dsn)
	t.Setenv("KAFKA_BROKERS", closedBroker)
	t.Setenv("HTTP_ADDR", address)
	t.Setenv("KAFKA_RETRY_INITIAL", "50ms")
	t.Setenv("KAFKA_RETRY_MAX", "100ms")
	runCtx, stop := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- run(runCtx) }()
	defer func() {
		stop()
		select {
		case err := <-runDone:
			if err != nil {
				t.Errorf("engagement shutdown: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("engagement did not stop")
		}
	}()

	client := &http.Client{Timeout: time.Second}
	baseURL := "http://" + address
	if err := waitForHTTP(ctx, func() (bool, error) {
		response, err := client.Get(baseURL + "/healthz")
		if err != nil {
			return false, nil
		}
		defer response.Body.Close()
		return response.StatusCode == http.StatusOK, nil
	}); err != nil {
		t.Fatalf("engagement did not become ready while Kafka was unavailable: %v", err)
	}
	response, err := client.Get(baseURL + "/stats/posts/42")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("stats status: got %d, want %d", response.StatusCode, http.StatusNotFound)
	}
}

func availableHTTPAddress(t *testing.T) string {
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

func waitForHTTP(ctx context.Context, check func() (bool, error)) error {
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
