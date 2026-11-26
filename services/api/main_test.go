package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"MicroBlog/services/api/internal/outbox"
	"MicroBlog/services/api/internal/testutil"
)

func TestApplicationExposesPprofEndpoint(t *testing.T) {
	app := newApplication(testutil.NewDB())

	request := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	response := httptest.NewRecorder()
	app.Handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("pprof status: got %d", response.Code)
	}
}

func TestRelayConfigFromEnv(t *testing.T) {
	t.Setenv("KAFKA_TOPIC", " custom.events ")
	t.Setenv("KAFKA_RETRY_INITIAL", "2s")
	t.Setenv("KAFKA_RETRY_MAX", "15s")
	t.Setenv("OUTBOX_POLL_INTERVAL", "500ms")
	t.Setenv("OUTBOX_BACKLOG_LOG_INTERVAL", "45s")

	config, err := relayConfigFromEnv()
	if err != nil {
		t.Fatalf("relayConfigFromEnv() error = %v", err)
	}
	if config.Topic != "custom.events" || config.InitialBackoff != 2*time.Second ||
		config.MaxBackoff != 15*time.Second || config.IdlePollInterval != 500*time.Millisecond ||
		config.BacklogInterval != 45*time.Second {
		t.Fatalf("relayConfigFromEnv() = %#v", config)
	}
}

func TestRelayConfigFromEnvRejectsInvalidDuration(t *testing.T) {
	t.Setenv("OUTBOX_POLL_INTERVAL", "0s")

	if _, err := relayConfigFromEnv(); err == nil {
		t.Fatal("relayConfigFromEnv() accepted a non-positive duration")
	}
}

func TestRelayConfigFromEnvDefaults(t *testing.T) {
	for _, key := range []string{
		"KAFKA_TOPIC", "KAFKA_RETRY_INITIAL", "KAFKA_RETRY_MAX",
		"OUTBOX_POLL_INTERVAL", "OUTBOX_BACKLOG_LOG_INTERVAL",
	} {
		t.Setenv(key, "")
	}

	config, err := relayConfigFromEnv()
	if err != nil {
		t.Fatalf("relayConfigFromEnv() error = %v", err)
	}
	want := outbox.Config{
		Topic:            outbox.TopicName,
		InitialBackoff:   time.Second,
		MaxBackoff:       30 * time.Second,
		IdlePollInterval: 250 * time.Millisecond,
		BacklogInterval:  30 * time.Second,
	}
	if config.Topic != want.Topic || config.InitialBackoff != want.InitialBackoff ||
		config.MaxBackoff != want.MaxBackoff || config.IdlePollInterval != want.IdlePollInterval ||
		config.BacklogInterval != want.BacklogInterval {
		t.Fatalf("relayConfigFromEnv() = %#v, want %#v", config, want)
	}
}
