package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStatsReturnsNotFoundWithoutProjection(t *testing.T) {
	response := request(New(fakeApplication{}), http.MethodGet, "/stats/posts/42")
	if response.Code != http.StatusNotFound {
		t.Fatalf("status: got %d, want %d", response.Code, http.StatusNotFound)
	}
}

func TestStatsReturnsZeroForCreatedPost(t *testing.T) {
	response := request(New(fakeApplication{found: true}), http.MethodGet, "/stats/posts/42")
	if response.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", response.Code, http.StatusOK)
	}
	var body struct {
		PostID    int64 `json:"post_id"`
		LikeCount int64 `json:"like_count"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.PostID != 42 || body.LikeCount != 0 {
		t.Fatalf("stats response: %+v", body)
	}
}

func TestHealthReportsDatabaseStatus(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		wantStatus int
	}{
		{name: "available", wantStatus: http.StatusOK},
		{name: "unavailable", err: errors.New("database unavailable"), wantStatus: http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := request(New(fakeApplication{healthErr: tc.err}), http.MethodGet, "/healthz")
			if response.Code != tc.wantStatus {
				t.Fatalf("status: got %d, want %d", response.Code, tc.wantStatus)
			}
		})
	}
}

func TestStatsRejectsInvalidPostID(t *testing.T) {
	response := request(New(fakeApplication{}), http.MethodGet, "/stats/posts/nope")
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d, want %d", response.Code, http.StatusBadRequest)
	}
}

type fakeApplication struct {
	count     int64
	found     bool
	statsErr  error
	healthErr error
}

func (a fakeApplication) PostLikeCount(context.Context, int64) (int64, bool, error) {
	return a.count, a.found, a.statsErr
}

func (a fakeApplication) Health(context.Context) error { return a.healthErr }

func request(handler http.Handler, method, path string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(method, path, nil))
	return response
}
