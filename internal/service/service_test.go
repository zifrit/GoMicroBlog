package service

import (
	"encoding/json"
	"sync"
	"testing"
)

func TestRegister(t *testing.T) {
	s := New()

	user, err := s.Register(" alice ")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if user.ID != "alice" || user.Username != "alice" {
		t.Fatalf("unexpected user: %+v", user)
	}

	if _, err := s.Register("alice"); err != ErrUserExists {
		t.Fatalf("expected duplicate user error, got %v", err)
	}
}

func TestListPostsReturnsSnapshotInsteadOfMutableStorage(t *testing.T) {
	s := New()
	if _, err := s.Register("alice"); err != nil {
		t.Fatalf("register: %v", err)
	}
	post, err := s.CreatePost("alice", "hello")
	if err != nil {
		t.Fatalf("create post: %v", err)
	}
	if _, err := s.LikePost(post.ID, "alice"); err != nil {
		t.Fatalf("like post: %v", err)
	}

	feed := s.ListPosts()
	feed[0].Text = "changed outside service"
	feed[0].Likes[0] = "mallory"

	stored := s.ListPosts()[0]
	if stored.Text != "hello" {
		t.Fatalf("post text was changed through feed: %q", stored.Text)
	}
	if stored.Likes[0] != "alice" {
		t.Fatalf("post likes were changed through feed: %#v", stored.Likes)
	}
}

func TestListPostsSerializesEmptyLikesAsArray(t *testing.T) {
	s := New()
	if _, err := s.Register("alice"); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := s.CreatePost("alice", "hello"); err != nil {
		t.Fatalf("create post: %v", err)
	}

	response, err := json.Marshal(s.ListPosts())
	if err != nil {
		t.Fatalf("marshal feed: %v", err)
	}
	if string(response) != `[{"id":1,"author":{"id":"alice","username":"alice"},"text":"hello","likes":[]}]` {
		t.Fatalf("unexpected feed JSON: %s", response)
	}
}

func TestCreatePostIsSafeForConcurrentRequests(t *testing.T) {
	const requests = 100
	s := New()
	if _, err := s.Register("alice"); err != nil {
		t.Fatalf("register: %v", err)
	}

	var group sync.WaitGroup
	for index := 0; index < requests; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, err := s.CreatePost("alice", "concurrent post"); err != nil {
				t.Errorf("create post: %v", err)
			}
		}()
	}
	group.Wait()

	if got := len(s.ListPosts()); got != requests {
		t.Fatalf("expected %d posts, got %d", requests, got)
	}
}

func BenchmarkCreatePost(b *testing.B) {
	s := New()
	if _, err := s.Register("alice"); err != nil {
		b.Fatalf("register: %v", err)
	}

	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := s.CreatePost("alice", "benchmark post"); err != nil {
			b.Fatalf("create post: %v", err)
		}
	}
}
