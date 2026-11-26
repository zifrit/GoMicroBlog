package events

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNewUserRegisteredEnvelope(t *testing.T) {
	occurredAt := time.Date(2026, time.November, 25, 15, 0, 0, 0, time.FixedZone("MSK", 3*60*60))
	event, err := NewUserRegistered("alice", occurredAt)
	if err != nil {
		t.Fatalf("NewUserRegistered: %v", err)
	}
	assertEnvelopeMetadata(t, event, "UserRegistered", occurredAt.UTC())
	assertExactJSONKeys(t, event.Payload, []string{"user_id"})
	if string(event.Payload) != `{"user_id":"alice"}` {
		t.Fatalf("payload: got %s", event.Payload)
	}
}

func TestNewPostCreatedEnvelope(t *testing.T) {
	occurredAt := time.Date(2026, time.November, 25, 12, 0, 0, 0, time.UTC)
	event, err := NewPostCreated(42, "alice", occurredAt)
	if err != nil {
		t.Fatalf("NewPostCreated: %v", err)
	}
	assertEnvelopeMetadata(t, event, "PostCreated", occurredAt)
	assertExactJSONKeys(t, event.Payload, []string{"post_id", "author_id"})
	if string(event.Payload) != `{"post_id":42,"author_id":"alice"}` {
		t.Fatalf("payload: got %s", event.Payload)
	}
}

func TestNewPostLikedEnvelope(t *testing.T) {
	occurredAt := time.Date(2026, time.November, 25, 12, 30, 0, 0, time.UTC)
	event, err := NewPostLiked(42, "alice", occurredAt)
	if err != nil {
		t.Fatalf("NewPostLiked: %v", err)
	}
	assertEnvelopeMetadata(t, event, "PostLiked", occurredAt)
	assertExactJSONKeys(t, event.Payload, []string{"post_id", "user_id"})
	if string(event.Payload) != `{"post_id":42,"user_id":"alice"}` {
		t.Fatalf("payload: got %s", event.Payload)
	}
}

func TestDecodeRejectsInvalidEnvelope(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{
			name: "unknown event type",
			data: `{"event_id":"00000000-0000-0000-0000-000000000001","event_type":"PostDeleted","schema_version":1,"occurred_at":"2026-11-25T12:00:00Z","producer":"api","payload":{}}`,
		},
		{
			name: "unsupported schema version",
			data: `{"event_id":"00000000-0000-0000-0000-000000000001","event_type":"PostLiked","schema_version":2,"occurred_at":"2026-11-25T12:00:00Z","producer":"api","payload":{"post_id":42,"user_id":"alice"}}`,
		},
		{
			name: "incomplete payload",
			data: `{"event_id":"00000000-0000-0000-0000-000000000001","event_type":"PostCreated","schema_version":1,"occurred_at":"2026-11-25T12:00:00Z","producer":"api","payload":{"post_id":42}}`,
		},
		{
			name: "post text is outside the event contract",
			data: `{"event_id":"00000000-0000-0000-0000-000000000001","event_type":"PostCreated","schema_version":1,"occurred_at":"2026-11-25T12:00:00Z","producer":"api","payload":{"post_id":42,"author_id":"alice","text":"private body"}}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Decode([]byte(test.data)); err == nil {
				t.Fatal("Decode accepted an invalid event")
			}
		})
	}
}

func TestMessageKey(t *testing.T) {
	tests := []struct {
		name     string
		event    Envelope
		want     string
		wantFail bool
	}{
		{
			name:  "user registration",
			event: Envelope{EventType: "UserRegistered", Payload: json.RawMessage(`{"user_id":"alice"}`)},
			want:  "user:alice",
		},
		{
			name:  "post creation",
			event: Envelope{EventType: "PostCreated", Payload: json.RawMessage(`{"post_id":42,"author_id":"alice"}`)},
			want:  "post:42",
		},
		{
			name:  "post like",
			event: Envelope{EventType: "PostLiked", Payload: json.RawMessage(`{"post_id":42,"user_id":"alice"}`)},
			want:  "post:42",
		},
		{
			name:     "unknown event",
			event:    Envelope{EventType: "PostDeleted", Payload: json.RawMessage(`{}`)},
			wantFail: true,
		},
		{
			name:     "missing user id",
			event:    Envelope{EventType: "UserRegistered", Payload: json.RawMessage(`{"user_id":""}`)},
			wantFail: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := MessageKey(test.event)
			if test.wantFail {
				if err == nil {
					t.Fatalf("MessageKey returned %q, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("MessageKey: %v", err)
			}
			if got != test.want {
				t.Fatalf("MessageKey: got %q, want %q", got, test.want)
			}
		})
	}
}

func assertEnvelopeMetadata(t *testing.T, event Envelope, eventType string, occurredAt time.Time) {
	t.Helper()
	if event.EventID == uuid.Nil {
		t.Fatal("event id is empty")
	}
	if event.EventType != eventType {
		t.Fatalf("event type: got %q, want %q", event.EventType, eventType)
	}
	if event.SchemaVersion != 1 {
		t.Fatalf("schema version: got %d, want 1", event.SchemaVersion)
	}
	if event.Producer != "api" {
		t.Fatalf("producer: got %q, want api", event.Producer)
	}
	if !event.OccurredAt.Equal(occurredAt) || event.OccurredAt.Location() != time.UTC {
		t.Fatalf("occurred_at: got %s, want UTC %s", event.OccurredAt, occurredAt)
	}

	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	assertExactJSONKeys(t, encoded, []string{"event_id", "event_type", "schema_version", "occurred_at", "producer", "payload"})
}

func assertExactJSONKeys(t *testing.T, raw json.RawMessage, want []string) {
	t.Helper()
	var value map[string]json.RawMessage
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("decode JSON keys: %v", err)
	}
	if len(value) != len(want) {
		t.Fatalf("JSON has %d fields, want %d: %s", len(value), len(want), raw)
	}
	for _, key := range want {
		if _, exists := value[key]; !exists {
			t.Errorf("JSON is missing field %q: %s", key, raw)
		}
	}
}
