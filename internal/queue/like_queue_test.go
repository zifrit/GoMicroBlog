package queue

import (
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
