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
	stop   chan struct{}
	pubs   sync.WaitGroup
	done   chan struct{}
}

func New(output io.Writer, bufferSize int) *Logger {
	logger := &Logger{
		events: make(chan string, bufferSize),
		writer: log.New(output, "microblog: ", log.LstdFlags),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	go logger.run()
	return logger
}

func (l *Logger) Publish(event string) {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.pubs.Add(1)
	l.mu.Unlock()
	defer l.pubs.Done()

	select {
	case l.events <- event:
	case <-l.stop:
	}
}

func (l *Logger) Close() {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	close(l.stop)
	l.mu.Unlock()
	l.pubs.Wait()
	close(l.events)
	<-l.done
}

func (l *Logger) run() {
	defer close(l.done)
	for event := range l.events {
		l.writer.Println(event)
	}
}
