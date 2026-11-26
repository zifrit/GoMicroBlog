package service

import (
	"context"
	"errors"
	"strings"

	"MicroBlog/services/api/internal/models"
	"MicroBlog/services/api/internal/repository"
)

var (
	ErrUserExists   = repository.ErrUserExists
	ErrUserNotFound = repository.ErrUserNotFound
	ErrPostNotFound = repository.ErrPostNotFound
	ErrAlreadyLiked = repository.ErrAlreadyLiked
	ErrInvalidUser  = errors.New("username is required")
	ErrInvalidPost  = errors.New("post text is required")
)

type Service struct {
	db repository.DB
}

// New получает хранилище API.
func New(db repository.DB) *Service {
	return &Service{db: db}
}

func (s *Service) Health(ctx context.Context) error {
	return s.db.Health(ctx)
}

func (s *Service) Register(ctx context.Context, username string) (*models.User, error) {
	username = strings.TrimSpace(username)
	if username == "" {
		return nil, ErrInvalidUser
	}
	user, err := s.db.Register(ctx, username)
	if err != nil {
		return nil, err
	}
	return user, nil
}

func (s *Service) CreatePost(ctx context.Context, username, text string) (*models.Post, error) {
	username = strings.TrimSpace(username)
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, ErrInvalidPost
	}
	post, err := s.db.CreatePost(ctx, username, text)
	if err != nil {
		return nil, err
	}
	return post, nil
}

func (s *Service) ListPosts(ctx context.Context) ([]*models.Post, error) {
	return s.db.ListPosts(ctx)
}

func (s *Service) LikePost(ctx context.Context, postID int64, username string) (*models.Post, error) {
	username = strings.TrimSpace(username)
	return s.db.LikePost(ctx, postID, username)
}
