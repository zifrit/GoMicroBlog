package queue

import (
	"context"
	"errors"
	"sync"
)

var ErrClosed = errors.New("like queue is closed")

type LikeJob struct {
	PostID   int
	Username string
}

type LikeQueue struct {
	jobs       chan LikeJob
	process    func(LikeJob) error
	onError    func(LikeJob, error)
	mu         sync.Mutex
	closed     bool
	stop       chan struct{} // Разблокирует отправителей при закрытии очереди.
	submitters sync.WaitGroup
	done       chan struct{}
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
	return q.SubmitContext(context.Background(), job)
}

// SubmitContext прекращает ожидание, если HTTP-запрос отменён или истёк его срок.
func (q *LikeQueue) SubmitContext(ctx context.Context, job LikeJob) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return ErrClosed
	}
	// Учитываем отправителя до снятия блокировки, чтобы Close дождался его.
	q.submitters.Add(1)
	q.mu.Unlock()
	defer q.submitters.Done()

	select {
	case q.jobs <- job:
		return nil
	case <-q.stop:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *LikeQueue) Close() {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	// Запрещаем новые задачи и пробуждаем тех, кто ждёт свободное место.
	q.closed = true
	close(q.stop)
	q.mu.Unlock()
	// Канал можно закрыть только после завершения всех отправителей.
	q.submitters.Wait()
	close(q.jobs)
	// Обработчик дочитывает принятые задачи и закрывает done.
	<-q.done
}

func (q *LikeQueue) run() {
	defer close(q.done)
	for job := range q.jobs {
		err := q.process(job)
		if err == nil {
			continue
		}
		if q.onError != nil {
			q.onError(job, err)
		}
	}
}
