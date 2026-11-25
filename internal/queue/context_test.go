package queue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCanceledSubmissionDoesNotWaitForFullQueueOrProcessJob(t *testing.T) {
	workerStarted := make(chan struct{})
	release := make(chan struct{})
	var processed []int
	q := NewLikeQueue(1, func(job LikeJob) error {
		if job.PostID == 1 {
			close(workerStarted)
			<-release
		}
		processed = append(processed, job.PostID)
		return nil
	}, nil)
	finish := sync.OnceFunc(func() { close(release); q.Close() })
	t.Cleanup(finish)
	if err := q.Submit(LikeJob{PostID: 1}); err != nil {
		t.Fatal(err)
	}
	<-workerStarted
	if err := q.Submit(LikeJob{PostID: 2}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- q.SubmitContext(ctx, LikeJob{PostID: 3}) }()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("canceled submit: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled submission is blocked by a full queue")
	}
	finish()
	if len(processed) != 2 || processed[0] != 1 || processed[1] != 2 {
		t.Fatalf("processed: %v", processed)
	}
}
