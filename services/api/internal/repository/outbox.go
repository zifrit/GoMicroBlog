package repository

import (
	"context"
	"fmt"
	"time"

	"MicroBlog/internal/events"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type OutboxRecord struct {
	ID         int64
	Event      events.Envelope
	MessageKey string
	Attempts   int
	CreatedAt  time.Time
	LastError  *string
}

type OutboxStore interface {
	NextPending(ctx context.Context) (OutboxRecord, bool, error)
	MarkPublished(ctx context.Context, id int64, at time.Time) error
	RecordFailure(ctx context.Context, id int64, publishErr error) error
	Backlog(ctx context.Context) (count int64, oldestPendingAt time.Time, err error)
}

func insertOutboxEvent(ctx context.Context, tx pgx.Tx, event events.Envelope) error {
	messageKey, err := events.MessageKey(event)
	if err != nil {
		return fmt.Errorf("create outbox message key: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO api.outbox_events (
			event_id, event_type, schema_version, message_key, payload, occurred_at
		) VALUES ($1, $2, $3, $4, $5::jsonb, $6)`,
		event.EventID, event.EventType, event.SchemaVersion, messageKey, string(event.Payload), event.OccurredAt)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}

func (db *Postgres) NextPending(ctx context.Context) (OutboxRecord, bool, error) {
	var record OutboxRecord
	var eventID uuid.UUID
	var eventType string
	var schemaVersion int
	var payload []byte
	err := db.pool.QueryRow(ctx, `
		SELECT id, event_id, event_type, schema_version, message_key, payload,
		       occurred_at, attempts, created_at, last_error
		FROM api.outbox_events
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT 1`).Scan(
		&record.ID,
		&eventID,
		&eventType,
		&schemaVersion,
		&record.MessageKey,
		&payload,
		&record.Event.OccurredAt,
		&record.Attempts,
		&record.CreatedAt,
		&record.LastError,
	)
	if err == pgx.ErrNoRows {
		return OutboxRecord{}, false, nil
	}
	if err != nil {
		return OutboxRecord{}, false, fmt.Errorf("select pending outbox event: %w", err)
	}
	record.Event = events.Envelope{
		EventID:       eventID,
		EventType:     eventType,
		SchemaVersion: schemaVersion,
		OccurredAt:    record.Event.OccurredAt,
		Producer:      events.ProducerAPI,
		Payload:       payload,
	}
	return record, true, nil
}

func (db *Postgres) MarkPublished(ctx context.Context, id int64, at time.Time) error {
	tag, err := db.pool.Exec(ctx, `
		UPDATE api.outbox_events
		SET published_at = $2, last_error = NULL
		WHERE id = $1 AND published_at IS NULL`, id, at.UTC())
	if err != nil {
		return fmt.Errorf("mark outbox event published: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("mark outbox event %d published: row is missing or already published", id)
	}
	return nil
}

func (db *Postgres) RecordFailure(ctx context.Context, id int64, publishErr error) error {
	if publishErr == nil {
		return fmt.Errorf("record outbox event %d failure: error is nil", id)
	}
	tag, err := db.pool.Exec(ctx, `
		UPDATE api.outbox_events
		SET attempts = attempts + 1, last_error = $2
		WHERE id = $1 AND published_at IS NULL`, id, publishErr.Error())
	if err != nil {
		return fmt.Errorf("record outbox event failure: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("record outbox event %d failure: row is missing or already published", id)
	}
	return nil
}

func (db *Postgres) Backlog(ctx context.Context) (int64, time.Time, error) {
	var count int64
	var oldest *time.Time
	err := db.pool.QueryRow(ctx, `
		SELECT count(*), min(created_at)
		FROM api.outbox_events
		WHERE published_at IS NULL`).Scan(&count, &oldest)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("read outbox backlog: %w", err)
	}
	if oldest == nil {
		return count, time.Time{}, nil
	}
	return count, *oldest, nil
}
