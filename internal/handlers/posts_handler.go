package handlers

import (
	"errors"
	"net/http"
	"strings"
)

type createPostRequest struct {
	Username string `json:"username"`
	Text     string `json:"text"`
}

func (request *createPostRequest) validate() error {
	if strings.TrimSpace(request.Username) == "" {
		return errors.New("username is required")
	}
	if strings.TrimSpace(request.Text) == "" {
		return errors.New("text is required")
	}
	return nil
}

func (h *Handler) posts(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		posts, err := h.service.ListPosts(r.Context())
		if err != nil {
			writeServiceError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, posts)
	case http.MethodPost:
		var request createPostRequest
		if !decodeJSON(w, r, &request) {
			return
		}

		post, err := h.service.CreatePost(r.Context(), request.Username, request.Text)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, post)
	default:
		methodNotAllowed(w)
	}
}
