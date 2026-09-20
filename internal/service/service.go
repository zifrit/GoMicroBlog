package service

import (
	"errors"
	"strings"
	"sync"

	"MicroBlog/internal/models"
	"MicroBlog/internal/syncutils"
)

var (
	ErrUserExists   = errors.New("user already exists")
	ErrUserNotFound = errors.New("user not found")
	ErrPostNotFound = errors.New("post not found")
	ErrAlreadyLiked = errors.New("post already liked by this user")
	ErrInvalidUser  = errors.New("username is required")
	ErrInvalidPost  = errors.New("post text is required")
)

type EventPublisher interface {
	Publish(string)
}

type Service struct {
	mu         sync.RWMutex
	users      map[string]*models.User
	posts      []*models.Post
	nextPostID syncutils.Counter
	events     EventPublisher
}

func New(events ...EventPublisher) *Service {
	var publisher EventPublisher
	if len(events) > 0 {
		publisher = events[0]
	}
	return &Service{
		users:  make(map[string]*models.User),
		posts:  make([]*models.Post, 0),
		events: publisher,
	}
}

func (s *Service) Register(username string) (*models.User, error) {
	username = strings.TrimSpace(username)
	if username == "" {
		return nil, ErrInvalidUser
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.users[username]; exists {
		return nil, ErrUserExists
	}

	user := &models.User{
		ID:       username,
		Username: username,
	}
	s.users[username] = user
	s.publish("user registered: " + username)
	return cloneUser(user), nil
}

func (s *Service) CreatePost(username string, text string) (*models.Post, error) {
	username = strings.TrimSpace(username)
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, ErrInvalidPost
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	user, exists := s.users[username]
	if !exists {
		return nil, ErrUserNotFound
	}

	post := &models.Post{
		ID:     int(s.nextPostID.Next()),
		Author: user,
		Text:   text,
		Likes:  make([]string, 0),
	}
	s.posts = append(s.posts, post)
	s.publish("post created: " + post.Text)
	return clonePost(post), nil
}

func (s *Service) ListPosts() []*models.Post {
	s.mu.RLock()
	defer s.mu.RUnlock()
	posts := make([]*models.Post, len(s.posts))
	for index, post := range s.posts {
		posts[index] = clonePost(post)
	}
	return posts
}

func (s *Service) LikePost(postID int, username string) (*models.Post, error) {
	username = strings.TrimSpace(username)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.users[username]; !exists {
		return nil, ErrUserNotFound
	}

	for _, post := range s.posts {
		if post.ID != postID {
			continue
		}
		for _, likedBy := range post.Likes {
			if likedBy == username {
				return nil, ErrAlreadyLiked
			}
		}
		post.Likes = append(post.Likes, username)
		s.publish("post liked: " + username)
		return clonePost(post), nil
	}

	return nil, ErrPostNotFound
}

func (s *Service) ValidateLike(postID int, username string) error {
	username = strings.TrimSpace(username)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, exists := s.users[username]; !exists {
		return ErrUserNotFound
	}
	for _, post := range s.posts {
		if post.ID == postID {
			return nil
		}
	}
	return ErrPostNotFound
}

func (s *Service) publish(event string) {
	if s.events != nil {
		s.events.Publish(event)
	}
}

func cloneUser(user *models.User) *models.User {
	if user == nil {
		return nil
	}
	cloneUser := *user
	return &cloneUser
}

func clonePost(post *models.Post) *models.Post {
	clonePost := *post
	clonePost.Author = cloneUser(post.Author)
	clonePost.Likes = make([]string, len(post.Likes))
	copy(clonePost.Likes, post.Likes)
	return &clonePost
}
