package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"MicroBlog/services/api/internal/service"
	"MicroBlog/services/api/internal/testutil"
)

func TestPostsFlow(t *testing.T) {
	handler := New(service.New(testutil.NewDB()))

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

	feed := request(handler, http.MethodGet, "/posts", "")
	if feed.Code != http.StatusOK {
		t.Fatalf("feed status: got %d", feed.Code)
	}
	if !strings.Contains(feed.Body.String(), `"text":"hello"`) {
		t.Fatalf("feed does not contain post: %s", feed.Body.String())
	}
	if !strings.Contains(feed.Body.String(), `"likes":["alice"]`) {
		t.Fatalf("feed does not contain the committed like: %s", feed.Body.String())
	}
}

func TestDuplicateLikeReturnsAccepted(t *testing.T) {
	handler := New(service.New(testutil.NewDB()))
	request(handler, http.MethodPost, "/register", `{"username":"alice"}`)
	request(handler, http.MethodPost, "/posts", `{"username":"alice","text":"hello"}`)
	first := request(handler, http.MethodPost, "/posts/1/like", `{"username":"alice"}`)
	second := request(handler, http.MethodPost, "/posts/1/like", `{"username":"alice"}`)
	if first.Code != http.StatusAccepted || second.Code != http.StatusAccepted || first.Body.String() != second.Body.String() {
		t.Fatalf("duplicate response: first=%d %s second=%d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
	}
}

func TestUnsupportedMethodReturnsMethodNotAllowed(t *testing.T) {
	handler := New(service.New(testutil.NewDB()))
	response := request(handler, http.MethodGet, "/register", "")

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status: got %d, want %d", response.Code, http.StatusMethodNotAllowed)
	}
	if response.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("Allow header: got %q, want %q", response.Header().Get("Allow"), http.MethodPost)
	}
}

func TestPostsMethodMismatchReturnsAllowedMethods(t *testing.T) {
	handler := New(service.New(testutil.NewDB()))
	response := request(handler, http.MethodPut, "/posts", "")

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status: got %d, want %d", response.Code, http.StatusMethodNotAllowed)
	}
	allow := response.Header().Get("Allow")
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPost} {
		if !strings.Contains(allow, method) {
			t.Fatalf("Allow header %q does not contain %q", allow, method)
		}
	}
}

func TestLikeMethodMismatchReturnsMethodNotAllowed(t *testing.T) {
	handler := New(service.New(testutil.NewDB()))
	response := request(handler, http.MethodGet, "/posts/1/like", "")

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status: got %d, want %d", response.Code, http.StatusMethodNotAllowed)
	}
	if response.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("Allow header: got %q, want %q", response.Header().Get("Allow"), http.MethodPost)
	}
}

func TestHeadUsesGetPostsRoute(t *testing.T) {
	handler := New(service.New(testutil.NewDB()))
	response := request(handler, http.MethodHead, "/posts", "")

	if response.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", response.Code, http.StatusOK)
	}
}

func TestUnknownPostActionReturnsNotFound(t *testing.T) {
	handler := New(service.New(testutil.NewDB()))
	response := request(handler, http.MethodGet, "/posts/1/comments", "")

	if response.Code != http.StatusNotFound {
		t.Fatalf("status: got %d, want %d", response.Code, http.StatusNotFound)
	}
}

func request(handler http.Handler, method string, path string, body string) *httptest.ResponseRecorder {
	record := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	handler.ServeHTTP(record, req)
	return record
}
