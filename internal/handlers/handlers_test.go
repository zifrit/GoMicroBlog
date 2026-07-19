package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"MicroBlog/internal/service"
)

func TestPostsFlow(t *testing.T) {
	handler := New(service.New())

	register := request(handler, http.MethodPost, "/register", `{"username":"alice"}`)
	if register.Code != http.StatusCreated {
		t.Fatalf("register status: got %d", register.Code)
	}

	createPost := request(handler, http.MethodPost, "/posts", `{"username":"alice","text":"hello"}`)
	if createPost.Code != http.StatusCreated {
		t.Fatalf("create post status: got %d", createPost.Code)
	}

	like := request(handler, http.MethodPost, "/posts/1/like", `{"username":"alice"}`)
	if like.Code != http.StatusOK {
		t.Fatalf("like status: got %d", like.Code)
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
