package service

import (
	"errors"
	"strings"

	"MicroBlog/internal/models"
)

var (
	ErrUserExists   = errors.New("user already exists")
	ErrUserNotFound = errors.New("user not found")
	ErrPostNotFound = errors.New("post not found")
	ErrAlreadyLiked = errors.New("post already liked by this user")
	ErrInvalidUser  = errors.New("username is required")
	ErrInvalidPost  = errors.New("post text is required")
)

// Service contains all application data in memory.
type Service struct {
	Users      map[string]*models.User
	Posts      []*models.Post
	nextPostID int
}

func New() *Service {
	return &Service{
		Users: make(map[string]*models.User),
		Posts: make([]*models.Post, 0),
	}
}

func (s *Service) Register(username string) (*models.User, error) {
	username = strings.TrimSpace(username)
	if username == "" {
		return nil, ErrInvalidUser
	}
	if _, exists := s.Users[username]; exists {
		return nil, ErrUserExists
	}

	user := &models.User{
		ID:       username,
		Username: username,
	}
	s.Users[username] = user
	return user, nil
}

func (s *Service) CreatePost(username string, text string) (*models.Post, error) {
	username = strings.TrimSpace(username)
	user, exists := s.Users[username]
	if !exists {
		return nil, ErrUserNotFound
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, ErrInvalidPost
	}

	s.nextPostID++
	post := &models.Post{
		ID:     s.nextPostID,
		Author: user,
		Text:   text,
		Likes:  make([]string, 0),
	}
	s.Posts = append(s.Posts, post)
	return post, nil
}

func (s *Service) ListPosts() []*models.Post {
	return s.Posts
}

func (s *Service) LikePost(postID int, username string) (*models.Post, error) {
	username = strings.TrimSpace(username)
	if _, exists := s.Users[username]; !exists {
		return nil, ErrUserNotFound
	}

	for _, post := range s.Posts {
		if post.ID != postID {
			continue
		}
		for _, likedBy := range post.Likes {
			if likedBy == username {
				return nil, ErrAlreadyLiked
			}
		}
		post.Likes = append(post.Likes, username)
		return post, nil
	}

	return nil, ErrPostNotFound
}
