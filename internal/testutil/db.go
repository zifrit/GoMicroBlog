// Package testutil предоставляет БД в памяти только для модульных тестов.
// Рабочее приложение всегда использует реализацию на PostgreSQL.
package testutil

import (
	"context"
	"sync"

	"MicroBlog/internal/models"
	"MicroBlog/internal/repository"
)

type DB struct {
	mu    sync.Mutex
	users map[string]*models.User
	posts []*models.Post
}

// NewDB создаёт пустую БД в памяти для модульных тестов.
func NewDB() *DB {
	return &DB{
		users: make(map[string]*models.User),
		posts: []*models.Post{},
	}
}

// Register добавляет пользователя, если его имя ещё свободно.
func (s *DB) Register(ctx context.Context, username string) (*models.User, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.users[username] != nil {
		return nil, repository.ErrUserExists
	}
	user := &models.User{ID: username, Username: username}
	s.users[username] = user
	result := *user
	return &result, nil
}

// CreatePost добавляет пост зарегистрированного пользователя.
func (s *DB) CreatePost(ctx context.Context, username, text string) (*models.Post, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	user := s.users[username]
	if user == nil {
		return nil, repository.ErrUserNotFound
	}
	post := &models.Post{ID: len(s.posts) + 1, Author: user, Text: text, Likes: []string{}}
	s.posts = append(s.posts, post)
	return clonePost(post), nil
}

// ListPosts возвращает копии постов, чтобы тесты не меняли данные напрямую.
func (s *DB) ListPosts(ctx context.Context) ([]*models.Post, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]*models.Post, len(s.posts))
	for i, post := range s.posts {
		result[i] = clonePost(post)
	}
	return result, nil
}

// LikePost добавляет единственный лайк от пользователя к посту.
func (s *DB) LikePost(ctx context.Context, id int, username string) (*models.Post, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validateLike(id, username); err != nil {
		return nil, err
	}
	post := s.posts[id-1]
	for _, user := range post.Likes {
		if user == username {
			return nil, repository.ErrAlreadyLiked
		}
	}
	post.Likes = append(post.Likes, username)
	return clonePost(post), nil
}

func (s *DB) ValidateLike(ctx context.Context, id int, username string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.validateLike(id, username)
}

func (s *DB) validateLike(id int, username string) error {
	if s.users[username] == nil {
		return repository.ErrUserNotFound
	}
	if id <= 0 || id > len(s.posts) {
		return repository.ErrPostNotFound
	}
	return nil
}

func clonePost(post *models.Post) *models.Post {
	result := *post
	author := *post.Author
	result.Author = &author
	result.Likes = append([]string{}, post.Likes...)
	return &result
}
