package service

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"MicroBlog/internal/models"
	"MicroBlog/internal/testutil"
)

func TestRegister(t *testing.T) {
	s := New(testutil.NewDB(), nil)

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
	s := New(testutil.NewDB(), nil)
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
	s := New(testutil.NewDB(), nil)
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
	s := New(testutil.NewDB(), nil)
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

type blockingEventPublisher struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *blockingEventPublisher) Publish(string) {
	p.once.Do(func() { close(p.started) })
	<-p.release
}

func TestServiceDoesNotHoldLockWhilePublishing(t *testing.T) {
	publisher := &blockingEventPublisher{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	s := New(testutil.NewDB(), publisher)

	registered := make(chan error)
	go func() {
		_, err := s.Register(context.Background(), "alice")
		registered <- err
	}()

	select {
	case <-publisher.started:
	case <-time.After(time.Second):
		t.Fatal("event publisher was not called")
	}

	listed := make(chan struct{})
	go func() {
		mustListPosts(t, s)
		close(listed)
	}()
	select {
	case <-listed:
	case <-time.After(time.Second):
		t.Fatal("ListPosts was blocked by event publishing")
	}

	close(publisher.release)
	select {
	case err := <-registered:
		if err != nil {
			t.Fatalf("register: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Register did not finish after publisher release")
	}
}

func BenchmarkCreatePost(b *testing.B) {
	s := New(testutil.NewDB(), nil)
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
