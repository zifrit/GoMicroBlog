package repository

import (
	"context"
	"errors"

	"MicroBlog/internal/models"
)

var (
	ErrUserExists   = errors.New("user already exists")
	ErrUserNotFound = errors.New("user not found")
	ErrPostNotFound = errors.New("post not found")
	ErrAlreadyLiked = errors.New("post already liked by this user")
)

// DB отвечает за хранение данных и атомарность каждой операции записи.
type DB interface {
	Register(ctx context.Context, username string) (*models.User, error)
	CreatePost(ctx context.Context, username string, text string) (*models.Post, error)
	ListPosts(ctx context.Context) ([]*models.Post, error)
	LikePost(ctx context.Context, postID int, username string) (*models.Post, error)
	ValidateLike(ctx context.Context, postID int, username string) error
}
