package handlers

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"MicroBlog/internal/queue"
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

	username := strings.TrimSpace(request.Username)
	if err := h.service.ValidateLike(r.Context(), postID, username); err != nil {
		writeServiceError(w, err)
		return
	}
	job := queue.LikeJob{
		PostID:   postID,
		Username: username,
	}
	err = h.likes.SubmitContext(r.Context(), job)
	if err != nil {
		queueClosed := errors.Is(err, queue.ErrClosed)
		requestCanceled := errors.Is(err, context.Canceled)
		requestTimedOut := errors.Is(err, context.DeadlineExceeded)
		if queueClosed || requestCanceled || requestTimedOut {
			writeError(w, http.StatusServiceUnavailable, "like queue is unavailable")
			return
		}
		writeError(w, http.StatusInternalServerError, "could not queue like")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "like accepted"})
}
