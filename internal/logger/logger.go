package logger

import (
	"io"
	"log"
	"sync"
)

type Logger struct {
	events chan string
	writer *log.Logger
	mu     sync.Mutex
	closed bool
	done   chan struct{}
}

func New(output io.Writer, bufferSize int) *Logger {
	logger := &Logger{
		events: make(chan string, bufferSize),
		writer: log.New(output, "microblog: ", log.LstdFlags),
		done:   make(chan struct{}),
	}
	go logger.run()
	return logger
}

func (l *Logger) Publish(event string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	l.events <- event
}

func (l *Logger) Close() {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	close(l.events)
	l.mu.Unlock()
	<-l.done
}

func (l *Logger) run() {
	defer close(l.done)
	for event := range l.events {
		l.writer.Println(event)
	}
}
