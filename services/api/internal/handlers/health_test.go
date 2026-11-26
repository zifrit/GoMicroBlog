package handlers

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func TestHealthzReportsDatabaseStatus(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		wantStatus int
	}{
		{name: "available", wantStatus: http.StatusOK},
		{name: "unavailable", err: errors.New("database unavailable"), wantStatus: http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := request(New(healthApplication{err: tc.err}), http.MethodGet, "/healthz", "")
			if response.Code != tc.wantStatus {
				t.Fatalf("health status: got %d, want %d", response.Code, tc.wantStatus)
			}
		})
	}
}

type healthApplication struct {
	Application
	err error
}

func (a healthApplication) Health(context.Context) error { return a.err }
