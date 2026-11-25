package service

import (
	"context"
	"errors"
	"strings"

	"MicroBlog/internal/models"
	"MicroBlog/internal/repository"
)

var (
	ErrUserExists   = repository.ErrUserExists
	ErrUserNotFound = repository.ErrUserNotFound
	ErrPostNotFound = repository.ErrPostNotFound
	ErrAlreadyLiked = repository.ErrAlreadyLiked
	ErrInvalidUser  = errors.New("username is required")
	ErrInvalidPost  = errors.New("post text is required")
)

type EventPublisher interface {
	Publish(string)
}

type Service struct {
	db     repository.DB
	events EventPublisher
}

// New получает БД и логгер. В тестах без событий вместо логгера передаётся nil.
func New(db repository.DB, events EventPublisher) *Service {
	return &Service{
		db:     db,
		events: events,
	}
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
	s.publish("user registered: " + username)
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
	s.publish("post created: " + post.Text)
	return post, nil
}

func (s *Service) ListPosts(ctx context.Context) ([]*models.Post, error) {
	return s.db.ListPosts(ctx)
}

func (s *Service) LikePost(ctx context.Context, postID int, username string) (*models.Post, error) {
	username = strings.TrimSpace(username)
	post, err := s.db.LikePost(ctx, postID, username)
	if err != nil {
		return nil, err
	}
	s.publish("post liked: " + username)
	return post, nil
}

func (s *Service) ValidateLike(ctx context.Context, postID int, username string) error {
	return s.db.ValidateLike(ctx, postID, strings.TrimSpace(username))
}

func (s *Service) publish(event string) {
	if s.events != nil {
		s.events.Publish(event)
	}
}
