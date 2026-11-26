package repository

import (
	"context"

	"MicroBlog/internal/events"
)

type Store interface {
	ApplyEvent(ctx context.Context, event events.Envelope) error
	PostLikeCount(ctx context.Context, postID int64) (count int64, found bool, err error)
	Health(ctx context.Context) error
}
