package queue

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestLikeQueueProcessesSubmittedJobInBackground(t *testing.T) {
	processed := make(chan LikeJob, 1)
	likes := NewLikeQueue(1, func(job LikeJob) error {
		processed <- job
		return nil
	}, nil)
	t.Cleanup(likes.Close)

	if err := likes.Submit(LikeJob{PostID: 7, Username: "alice"}); err != nil {
		t.Fatalf("submit like: %v", err)
	}

	select {
	case job := <-processed:
		if job.PostID != 7 || job.Username != "alice" {
			t.Fatalf("unexpected job: %+v", job)
		}
	case <-time.After(time.Second):
		t.Fatal("like job was not processed")
	}
}

func TestLikeQueueCloseUnblocksSubmitAndDrainsAcceptedJobs(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	processed := make(chan LikeJob, 2)
	var once sync.Once
	likes := NewLikeQueue(1, func(job LikeJob) error {
		once.Do(func() { close(started) })
		<-release
		processed <- job
		return nil
	}, nil)

	likes.Submit(LikeJob{PostID: 1, Username: "alice"})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("queue worker did not start processing")
	}
	if err := likes.Submit(LikeJob{PostID: 2, Username: "bob"}); err != nil {
		t.Fatalf("submit accepted job: %v", err)
	}

	thirdSubmitted := make(chan error)
	go func() {
		thirdSubmitted <- likes.Submit(LikeJob{PostID: 3, Username: "carol"})
	}()

	select {
	case err := <-thirdSubmitted:
		t.Fatalf("third submit returned before shutdown: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	closed := make(chan struct{})
	go func() {
		likes.Close()
		close(closed)
	}()

	select {
	case err := <-thirdSubmitted:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("third submit error: got %v, want %v", err, ErrClosed)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock the pending submit")
	}

	select {
	case <-closed:
		t.Fatal("Close returned while a job was still processing")
	case <-time.After(20 * time.Millisecond):
	}

	close(release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after the worker was released")
	}

	got := []LikeJob{<-processed, <-processed}
	if got[0].PostID != 1 || got[1].PostID != 2 {
		t.Fatalf("accepted jobs were not drained: %+v", got)
	}
	likes.Close()
}
