package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestApplicationExposesPprofEndpoint(t *testing.T) {
	app := newApplication(&bytes.Buffer{})
	t.Cleanup(app.Close)

	request := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	response := httptest.NewRecorder()
	app.Handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("pprof status: got %d", response.Code)
	}
}
