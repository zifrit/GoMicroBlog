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
	onError func(LikeJob, error)
	mu      sync.Mutex
	closed  bool
	stop    chan struct{}
	submits sync.WaitGroup
	done    chan struct{}
}

func NewLikeQueue(bufferSize int, process func(LikeJob) error, onError func(LikeJob, error)) *LikeQueue {
	queue := &LikeQueue{
		jobs:    make(chan LikeJob, bufferSize),
		process: process,
		onError: onError,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go queue.run()
	return queue
}

func (q *LikeQueue) Submit(job LikeJob) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return ErrClosed
	}
	q.submits.Add(1)
	q.mu.Unlock()
	defer q.submits.Done()

	select {
	case q.jobs <- job:
		return nil
	case <-q.stop:
		return ErrClosed
	}
}

func (q *LikeQueue) Close() {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.closed = true
	close(q.stop)
	q.mu.Unlock()
	q.submits.Wait()
	close(q.jobs)
	<-q.done
}

func (q *LikeQueue) run() {
	defer close(q.done)
	for job := range q.jobs {
		if err := q.process(job); err != nil && q.onError != nil {
			q.onError(job, err)
		}
	}
}
