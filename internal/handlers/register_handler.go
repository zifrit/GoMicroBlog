package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"MicroBlog/internal/models"
	"MicroBlog/internal/queue"
	"MicroBlog/internal/service"
)

type Application interface {
	Register(context.Context, string) (*models.User, error)
	CreatePost(context.Context, string, string) (*models.Post, error)
	ListPosts(context.Context) ([]*models.Post, error)
	ValidateLike(context.Context, int, string) error
}

type LikeSubmitter interface {
	SubmitContext(context.Context, queue.LikeJob) error
}

type Handler struct {
	service Application
	likes   LikeSubmitter
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

func New(appService Application, likes LikeSubmitter) http.Handler {
	h := &Handler{service: appService, likes: likes}
	mux := http.NewServeMux()
	mux.HandleFunc("/register", h.register)
	mux.HandleFunc("/posts", h.posts)
	mux.HandleFunc("/posts/", h.postAction)
	return mux
}

func (request *registerRequest) validate() error {
	if strings.TrimSpace(request.Username) == "" {
		return errors.New("username is required")
	}
	return nil
}

func (h *Handler) register(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}

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
	switch {
	case errors.Is(err, service.ErrUserNotFound), errors.Is(err, service.ErrPostNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, service.ErrUserExists), errors.Is(err, service.ErrAlreadyLiked),
		errors.Is(err, service.ErrInvalidUser), errors.Is(err, service.ErrInvalidPost):
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "internal server error")
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}

func methodNotAllowed(w http.ResponseWriter) {
	writeError(w, http.StatusMethodNotAllowed, "method not allowed")
}
