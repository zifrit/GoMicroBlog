package service

import (
	"context"
	"errors"
	"testing"

	"MicroBlog/internal/models"
	"MicroBlog/internal/repository"
)

type unavailableDB struct {
	repository.DB
	err error
}

func (s unavailableDB) ListPosts(ctx context.Context) ([]*models.Post, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, s.err
}

func TestListPostsPropagatesStorageFailure(t *testing.T) {
	failure := errors.New("database unavailable")
	s := New(unavailableDB{err: failure}, nil)
	if _, err := s.ListPosts(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("storage failure: %v", err)
	}
}

func TestListPostsPropagatesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := New(unavailableDB{}, nil)
	if _, err := s.ListPosts(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}
