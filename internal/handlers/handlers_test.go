package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"MicroBlog/internal/queue"
	"MicroBlog/internal/service"
)

func TestPostsFlow(t *testing.T) {
	appService := service.New()
	likes := queue.NewLikeQueue(1, func(job queue.LikeJob) error {
		_, err := appService.LikePost(job.PostID, job.Username)
		return err
	}, nil)
	t.Cleanup(likes.Close)
	handler := New(appService, likes)

	register := request(handler, http.MethodPost, "/register", `{"username":"alice"}`)
	if register.Code != http.StatusCreated {
		t.Fatalf("register status: got %d", register.Code)
	}

	createPost := request(handler, http.MethodPost, "/posts", `{"username":"alice","text":"hello"}`)
	if createPost.Code != http.StatusCreated {
		t.Fatalf("create post status: got %d", createPost.Code)
	}

	like := request(handler, http.MethodPost, "/posts/1/like", `{"username":"alice"}`)
	if like.Code != http.StatusAccepted {
		t.Fatalf("like status: got %d", like.Code)
	}

	deadline := time.After(time.Second)
	for {
		feed := request(handler, http.MethodGet, "/posts", "")
		if strings.Contains(feed.Body.String(), `"likes":["alice"]`) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("like was not processed: %s", feed.Body.String())
		case <-time.After(time.Millisecond):
		}
	}

	feed := request(handler, http.MethodGet, "/posts", "")
	if feed.Code != http.StatusOK {
		t.Fatalf("feed status: got %d", feed.Code)
	}
	if !strings.Contains(feed.Body.String(), `"text":"hello"`) {
		t.Fatalf("feed does not contain post: %s", feed.Body.String())
	}
}

func request(handler http.Handler, method string, path string, body string) *httptest.ResponseRecorder {
	record := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	handler.ServeHTTP(record, req)
	return record
}
