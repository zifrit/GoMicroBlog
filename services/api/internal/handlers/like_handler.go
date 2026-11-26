package handlers

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"MicroBlog/services/api/internal/service"
)

type likeRequest struct {
	Username string `json:"username"`
}

func (request *likeRequest) validate() error {
	if strings.TrimSpace(request.Username) == "" {
		return errors.New("username is required")
	}
	return nil
}

func (h *Handler) likePost(w http.ResponseWriter, r *http.Request) {
	postID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "post id must be a number")
		return
	}

	var request likeRequest
	if !decodeJSON(w, r, &request) {
		return
	}

	username := strings.TrimSpace(request.Username)
	_, err = h.service.LikePost(r.Context(), postID, username)
	if errors.Is(err, service.ErrAlreadyLiked) {
		writeLikeAccepted(w)
		return
	}
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeLikeAccepted(w)
}

func writeLikeAccepted(w http.ResponseWriter) {
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "like accepted"})
}
