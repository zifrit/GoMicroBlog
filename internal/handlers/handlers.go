package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"MicroBlog/internal/service"
)

type Handler struct {
	service *service.Service
}

type registerRequest struct {
	Username string `json:"username"`
}

type createPostRequest struct {
	Username string `json:"username"`
	Text     string `json:"text"`
}

type likeRequest struct {
	Username string `json:"username"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func New(s *service.Service) http.Handler {
	h := &Handler{service: s}
	mux := http.NewServeMux()
	mux.HandleFunc("/register", h.register)
	mux.HandleFunc("/posts", h.posts)
	mux.HandleFunc("/posts/", h.postAction)
	return mux
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

func (h *Handler) posts(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, h.service.ListPosts())
	case http.MethodPost:
		var request createPostRequest
		if !decodeJSON(w, r, &request) {
			return
		}

		post, err := h.service.CreatePost(request.Username, request.Text)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, post)
	default:
		methodNotAllowed(w)
	}
}

func (h *Handler) postAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 3 || parts[0] != "posts" || parts[2] != "like" {
		http.NotFound(w, r)
		return
	}

	postID, err := strconv.Atoi(parts[1])
	if err != nil {
		writeError(w, http.StatusBadRequest, "post id must be a number")
		return
	}

	var request likeRequest
	if !decodeJSON(w, r, &request) {
		return
	}

	post, err := h.service.LikePost(postID, strings.TrimSpace(request.Username))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, post)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, value interface{}) bool {
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(value); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
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

func writeJSON(w http.ResponseWriter, status int, value interface{}) {
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
