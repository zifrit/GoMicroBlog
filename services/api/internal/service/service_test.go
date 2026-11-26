package service

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"MicroBlog/services/api/internal/models"
	"MicroBlog/services/api/internal/testutil"
)

func TestRegister(t *testing.T) {
	s := New(testutil.NewDB())

	user, err := s.Register(context.Background(), " alice ")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if user.ID != "alice" || user.Username != "alice" {
		t.Fatalf("unexpected user: %+v", user)
	}

	if _, err := s.Register(context.Background(), "alice"); err != ErrUserExists {
		t.Fatalf("expected duplicate user error, got %v", err)
	}
}

func TestListPostsReturnsSnapshotInsteadOfMutableStorage(t *testing.T) {
	s := New(testutil.NewDB())
	if _, err := s.Register(context.Background(), "alice"); err != nil {
		t.Fatalf("register: %v", err)
	}
	post, err := s.CreatePost(context.Background(), "alice", "hello")
	if err != nil {
		t.Fatalf("create post: %v", err)
	}
	if _, err := s.LikePost(context.Background(), post.ID, "alice"); err != nil {
		t.Fatalf("like post: %v", err)
	}

	feed := mustListPosts(t, s)
	feed[0].Text = "changed outside service"
	feed[0].Likes[0] = "mallory"

	savedPost := mustListPosts(t, s)[0]
	if savedPost.Text != "hello" {
		t.Fatalf("post text was changed through feed: %q", savedPost.Text)
	}
	if savedPost.Likes[0] != "alice" {
		t.Fatalf("post likes were changed through feed: %#v", savedPost.Likes)
	}
}

func TestListPostsSerializesEmptyLikesAsArray(t *testing.T) {
	s := New(testutil.NewDB())
	if _, err := s.Register(context.Background(), "alice"); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := s.CreatePost(context.Background(), "alice", "hello"); err != nil {
		t.Fatalf("create post: %v", err)
	}

	response, err := json.Marshal(mustListPosts(t, s))
	if err != nil {
		t.Fatalf("marshal feed: %v", err)
	}
	if string(response) != `[{"id":1,"author":{"id":"alice","username":"alice"},"text":"hello","likes":[]}]` {
		t.Fatalf("unexpected feed JSON: %s", response)
	}
}

func TestCreatePostIsSafeForConcurrentRequests(t *testing.T) {
	const requests = 100
	s := New(testutil.NewDB())
	if _, err := s.Register(context.Background(), "alice"); err != nil {
		t.Fatalf("register: %v", err)
	}

	var group sync.WaitGroup
	for index := 0; index < requests; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, err := s.CreatePost(context.Background(), "alice", "concurrent post"); err != nil {
				t.Errorf("create post: %v", err)
			}
		}()
	}
	group.Wait()

	if got := len(mustListPosts(t, s)); got != requests {
		t.Fatalf("expected %d posts, got %d", requests, got)
	}
}

func BenchmarkCreatePost(b *testing.B) {
	s := New(testutil.NewDB())
	if _, err := s.Register(context.Background(), "alice"); err != nil {
		b.Fatalf("register: %v", err)
	}

	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := s.CreatePost(context.Background(), "alice", "benchmark post"); err != nil {
			b.Fatalf("create post: %v", err)
		}
	}
}

func mustListPosts(t testing.TB, s *Service) []*models.Post {
	t.Helper()
	posts, err := s.ListPosts(context.Background())
	if err != nil {
		t.Fatalf("list posts: %v", err)
	}
	return posts
}
