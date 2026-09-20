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
	users      map[string]*models.User
	posts      []*models.Post
	nextPostID int
}

func New() *Service {
	return &Service{
		users: make(map[string]*models.User),
		posts: make([]*models.Post, 0),
	}
}

func (s *Service) Register(username string) (*models.User, error) {
	username = strings.TrimSpace(username)
	if username == "" {
		return nil, ErrInvalidUser
	}
	if _, exists := s.users[username]; exists {
		return nil, ErrUserExists
	}

	user := &models.User{
		ID:       username,
		Username: username,
	}
	s.users[username] = user
	return cloneUser(user), nil
}

func (s *Service) CreatePost(username string, text string) (*models.Post, error) {
	username = strings.TrimSpace(username)
	user, exists := s.users[username]
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
	s.posts = append(s.posts, post)
	return clonePost(post), nil
}

func (s *Service) ListPosts() []*models.Post {
	posts := make([]*models.Post, 0, len(s.posts))
	for _, post := range s.posts {
		posts = append(posts, clonePost(post))
	}
	return posts
}

func (s *Service) LikePost(postID int, username string) (*models.Post, error) {
	username = strings.TrimSpace(username)
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
		return clonePost(post), nil
	}

	return nil, ErrPostNotFound
}

func cloneUser(user *models.User) *models.User {
	if user == nil {
		return nil
	}
	cloneUser := *user
	return &cloneUser
}

func clonePost(post *models.Post) *models.Post {
	if post == nil {
		return nil
	}
	clonePost := *post
	clonePost.Author = cloneUser(post.Author)
	clonePost.Likes = append([]string(nil), post.Likes...)
	return &clonePost
}
