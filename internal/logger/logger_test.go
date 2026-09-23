package logger

import (
	"bytes"
	"strings"
	"testing"
)

func TestLoggerWritesPublishedEventsWhenClosed(t *testing.T) {
	var output bytes.Buffer
	events := New(&output, 2)

	events.Publish("user registered: alice")
	events.Publish("post created: hello")
	events.Close()

	log := output.String()
	if !strings.Contains(log, "user registered: alice") {
		t.Fatalf("registration event was not logged: %q", log)
	}
	if !strings.Contains(log, "post created: hello") {
		t.Fatalf("post event was not logged: %q", log)
	}
}
