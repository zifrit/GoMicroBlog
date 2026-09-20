package queue

import (
	"errors"
	"sync"
)

var ErrClosed = errors.New("like queue is closed")

type LikeJob struct {
	PostID   int
	Username string
}

type LikeQueue struct {
	jobs    chan LikeJob
	process func(LikeJob) error
	mu      sync.RWMutex
	closed  bool
	done    chan struct{}
}

func NewLikeQueue(bufferSize int, process func(LikeJob) error) *LikeQueue {
	queue := &LikeQueue{
		jobs:    make(chan LikeJob, bufferSize),
		process: process,
		done:    make(chan struct{}),
	}
	go queue.run()
	return queue
}

func (q *LikeQueue) Submit(job LikeJob) error {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return ErrClosed
	}
	q.jobs <- job
	return nil
}

func (q *LikeQueue) Close() {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.closed = true
	close(q.jobs)
	q.mu.Unlock()
	<-q.done
}

func (q *LikeQueue) run() {
	defer close(q.done)
	for job := range q.jobs {
		_ = q.process(job)
	}
}
