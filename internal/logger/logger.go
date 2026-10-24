package logger

import (
	"io"
	"log"
	"sync"
)

type Logger struct {
	events     chan string
	writer     *log.Logger
	mu         sync.Mutex
	closed     bool
	stop       chan struct{}
	publishers sync.WaitGroup
	done       chan struct{}
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
	// Close должен дождаться каждого отправителя перед закрытием канала.
	l.publishers.Add(1)
	l.mu.Unlock()
	defer l.publishers.Done()

	// Отправляем без блокировки mu, иначе Close не сможет остановить ожидание.
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
	// Новые события больше не принимаются; заблокированные отправители просыпаются.
	l.closed = true
	close(l.stop)
	l.mu.Unlock()
	l.publishers.Wait()
	close(l.events)
	// Ждём записи всех уже принятых событий.
	<-l.done
}

func (l *Logger) run() {
	defer close(l.done)
	for event := range l.events {
		l.writer.Println(event)
	}
}
