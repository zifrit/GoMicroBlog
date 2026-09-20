package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"MicroBlog/internal/models"
	"MicroBlog/internal/queue"
	"MicroBlog/internal/service"
)

type Application interface {
	Register(string) (*models.User, error)
	CreatePost(string, string) (*models.Post, error)
	ListPosts() []*models.Post
	ValidateLike(int, string) error
}

type LikeSubmitter interface {
	Submit(queue.LikeJob) error
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

func New(s Application, likes LikeSubmitter) http.Handler {
	h := &Handler{service: s, likes: likes}
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

	user, err := h.service.Register(request.Username)
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
	status := http.StatusBadRequest
	if errors.Is(err, service.ErrUserNotFound) || errors.Is(err, service.ErrPostNotFound) {
		status = http.StatusNotFound
	}
	writeError(w, status, err.Error())
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
