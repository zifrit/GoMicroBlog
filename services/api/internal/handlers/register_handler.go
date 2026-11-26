package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"MicroBlog/services/api/internal/models"
	"MicroBlog/services/api/internal/service"
)

type Application interface {
	Health(context.Context) error
	Register(context.Context, string) (*models.User, error)
	CreatePost(context.Context, string, string) (*models.Post, error)
	ListPosts(context.Context) ([]*models.Post, error)
	LikePost(context.Context, int64, string) (*models.Post, error)
}

type Handler struct {
	service Application
}

type registerRequest struct {
	Username string `json:"username"`
}

type requestPayload interface {
	validate() error
}

type errorResponse struct {
	Error string `json:"error"`
}

func New(appService Application) http.Handler {
	h := &Handler{service: appService}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /register", h.register)
	mux.HandleFunc("GET /healthz", h.healthz)
	mux.HandleFunc("GET /posts", h.listPosts)
	mux.HandleFunc("POST /posts", h.createPost)
	mux.HandleFunc("POST /posts/{id}/like", h.likePost)
	return mux
}

func (request *registerRequest) validate() error {
	if strings.TrimSpace(request.Username) == "" {
		return errors.New("username is required")
	}
	return nil
}

func (h *Handler) register(w http.ResponseWriter, r *http.Request) {
	var request registerRequest
	if !decodeJSON(w, r, &request) {
		return
	}

	user, err := h.service.Register(r.Context(), request.Username)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, user)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, value requestPayload) bool {
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(value); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return false
	}
	if err := value.validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return false
	}
	return true
}

func writeServiceError(w http.ResponseWriter, err error) {
	if errors.Is(err, service.ErrUserNotFound) || errors.Is(err, service.ErrPostNotFound) {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	if errors.Is(err, service.ErrUserExists) || errors.Is(err, service.ErrAlreadyLiked) ||
		errors.Is(err, service.ErrInvalidUser) || errors.Is(err, service.ErrInvalidPost) {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeError(w, http.StatusInternalServerError, "internal server error")
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}
