package events

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	UserRegistered = "UserRegistered"
	PostCreated    = "PostCreated"
	PostLiked      = "PostLiked"

	SchemaVersion = 1
	ProducerAPI   = "api"
)

// Envelope is the versioned wire contract shared by API and engagement.
type Envelope struct {
	EventID       uuid.UUID       `json:"event_id"`
	EventType     string          `json:"event_type"`
	SchemaVersion int             `json:"schema_version"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Producer      string          `json:"producer"`
	Payload       json.RawMessage `json:"payload"`
}

type UserRegisteredPayload struct {
	UserID string `json:"user_id"`
}

type PostCreatedPayload struct {
	PostID   int64  `json:"post_id"`
	AuthorID string `json:"author_id"`
}

type PostLikedPayload struct {
	PostID int64  `json:"post_id"`
	UserID string `json:"user_id"`
}

func NewUserRegistered(userID string, occurredAt time.Time) (Envelope, error) {
	if strings.TrimSpace(userID) == "" {
		return Envelope{}, errors.New("user_id is required")
	}
	return newEnvelope(UserRegistered, UserRegisteredPayload{UserID: userID}, occurredAt)
}

func NewPostCreated(postID int64, authorID string, occurredAt time.Time) (Envelope, error) {
	if postID <= 0 {
		return Envelope{}, errors.New("post_id must be positive")
	}
	if strings.TrimSpace(authorID) == "" {
		return Envelope{}, errors.New("author_id is required")
	}
	return newEnvelope(PostCreated, PostCreatedPayload{PostID: postID, AuthorID: authorID}, occurredAt)
}

func NewPostLiked(postID int64, userID string, occurredAt time.Time) (Envelope, error) {
	if postID <= 0 {
		return Envelope{}, errors.New("post_id must be positive")
	}
	if strings.TrimSpace(userID) == "" {
		return Envelope{}, errors.New("user_id is required")
	}
	return newEnvelope(PostLiked, PostLikedPayload{PostID: postID, UserID: userID}, occurredAt)
}

func newEnvelope(eventType string, payload any, occurredAt time.Time) (Envelope, error) {
	if occurredAt.IsZero() {
		return Envelope{}, errors.New("occurred_at is required")
	}
	eventID, err := uuid.NewRandom()
	if err != nil {
		return Envelope{}, fmt.Errorf("create event id: %w", err)
	}
	encodedPayload, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("encode event payload: %w", err)
	}
	return Envelope{
		EventID:       eventID,
		EventType:     eventType,
		SchemaVersion: SchemaVersion,
		OccurredAt:    occurredAt.UTC(),
		Producer:      ProducerAPI,
		Payload:       encodedPayload,
	}, nil
}

// Decode validates the envelope and the exact payload fields for schema version 1.
func Decode(data []byte) (Envelope, error) {
	var envelope Envelope
	if err := decodeStrict(data, &envelope); err != nil {
		return Envelope{}, fmt.Errorf("decode event envelope: %w", err)
	}
	if envelope.EventID == uuid.Nil {
		return Envelope{}, errors.New("event_id is required")
	}
	if envelope.SchemaVersion != SchemaVersion {
		return Envelope{}, fmt.Errorf("unsupported schema_version %d", envelope.SchemaVersion)
	}
	if envelope.OccurredAt.IsZero() {
		return Envelope{}, errors.New("occurred_at is required")
	}
	if envelope.Producer != ProducerAPI {
		return Envelope{}, fmt.Errorf("unsupported producer %q", envelope.Producer)
	}
	if _, err := decodePayload(envelope.EventType, envelope.Payload); err != nil {
		return Envelope{}, err
	}
	return envelope, nil
}

func MessageKey(envelope Envelope) (string, error) {
	payload, err := decodePayload(envelope.EventType, envelope.Payload)
	if err != nil {
		return "", err
	}
	switch value := payload.(type) {
	case UserRegisteredPayload:
		return "user:" + value.UserID, nil
	case PostCreatedPayload:
		return fmt.Sprintf("post:%d", value.PostID), nil
	case PostLikedPayload:
		return fmt.Sprintf("post:%d", value.PostID), nil
	default:
		return "", fmt.Errorf("unsupported event type %q", envelope.EventType)
	}
}

func decodePayload(eventType string, payload json.RawMessage) (any, error) {
	switch eventType {
	case UserRegistered:
		var value UserRegisteredPayload
		if err := decodeStrict(payload, &value); err != nil {
			return nil, fmt.Errorf("decode UserRegistered payload: %w", err)
		}
		if strings.TrimSpace(value.UserID) == "" {
			return nil, errors.New("UserRegistered payload requires user_id")
		}
		return value, nil
	case PostCreated:
		var value PostCreatedPayload
		if err := decodeStrict(payload, &value); err != nil {
			return nil, fmt.Errorf("decode PostCreated payload: %w", err)
		}
		if value.PostID <= 0 || strings.TrimSpace(value.AuthorID) == "" {
			return nil, errors.New("PostCreated payload requires a positive post_id and author_id")
		}
		return value, nil
	case PostLiked:
		var value PostLikedPayload
		if err := decodeStrict(payload, &value); err != nil {
			return nil, fmt.Errorf("decode PostLiked payload: %w", err)
		}
		if value.PostID <= 0 || strings.TrimSpace(value.UserID) == "" {
			return nil, errors.New("PostLiked payload requires a positive post_id and user_id")
		}
		return value, nil
	default:
		return nil, fmt.Errorf("unsupported event type %q", eventType)
	}
}

func decodeStrict(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extraValue any
	err := decoder.Decode(&extraValue)
	if err == nil {
		return errors.New("unexpected trailing JSON value")
	}
	if !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}
