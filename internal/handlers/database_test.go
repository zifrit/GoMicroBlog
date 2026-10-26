package handlers

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"MicroBlog/internal/models"
)

type failingApplication struct{ Application }

func (failingApplication) ListPosts(context.Context) ([]*models.Post, error) {
	return nil, errors.New("postgres password=secret: connection refused")
}

func TestFeedDatabaseFailureReturnsInternalServerError(t *testing.T) {
	response := request(New(failingApplication{}, nil), http.MethodGet, "/posts", "")
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status: %d", response.Code)
	}
	if strings.Contains(response.Body.String(), "secret") || !strings.Contains(response.Body.String(), "internal server error") {
		t.Fatalf("response: %s", response.Body.String())
	}
}
