package logger

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"
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

type blockingWriter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	output  bytes.Buffer
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.output.Write(p)
}

func (w *blockingWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.output.String()
}

func TestLoggerCloseUnblocksPublishAndDrainsAcceptedEvents(t *testing.T) {
	writer := &blockingWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	events := New(writer, 1)

	events.Publish("first")
	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("logger worker did not start writing")
	}
	events.Publish("second")

	thirdPublished := make(chan struct{})
	go func() {
		events.Publish("third")
		close(thirdPublished)
	}()

	select {
	case <-thirdPublished:
		t.Fatal("third publish was not blocked by the full buffer")
	case <-time.After(20 * time.Millisecond):
	}

	closed := make(chan struct{})
	go func() {
		events.Close()
		close(closed)
	}()

	select {
	case <-closed:
		t.Fatal("Close returned while the writer was blocked")
	case <-time.After(20 * time.Millisecond):
	}
	select {
	case <-thirdPublished:
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock the pending publish")
	}

	close(writer.release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after the writer was released")
	}

	events.Close()
	output := writer.String()
	if !strings.Contains(output, "first") || !strings.Contains(output, "second") {
		t.Fatalf("accepted events were not drained: %q", output)
	}
}
