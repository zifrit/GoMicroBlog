package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

type Application interface {
	PostLikeCount(ctx context.Context, postID int64) (count int64, found bool, err error)
	Health(ctx context.Context) error
}

type Handler struct {
	app Application
}

type statsResponse struct {
	PostID    int64 `json:"post_id"`
	LikeCount int64 `json:"like_count"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func New(app Application) http.Handler {
	handler := &Handler{app: app}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stats/posts/{id}", handler.postStats)
	mux.HandleFunc("GET /healthz", handler.healthz)
	return mux
}

func (h *Handler) postStats(w http.ResponseWriter, r *http.Request) {
	postID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || postID <= 0 {
		writeError(w, http.StatusBadRequest, "post id must be a positive integer")
		return
	}
	count, found, err := h.app.PostLikeCount(r.Context(), postID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "post stats not found")
		return
	}
	writeJSON(w, http.StatusOK, statsResponse{PostID: postID, LikeCount: count})
}

func (h *Handler) healthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := h.app.Health(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}
