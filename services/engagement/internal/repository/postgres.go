package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"MicroBlog/internal/events"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
}

func (db *Postgres) ApplyEvent(ctx context.Context, event events.Envelope) error {
	encoded, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode event envelope: %w", err)
	}
	validated, err := events.Decode(encoded)
	if err != nil {
		return fmt.Errorf("validate event envelope: %w", err)
	}

	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin apply event transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// ID события и счётчик сохраняются вместе. Повтор того же event_id не меняет счётчик.
	tag, err := tx.Exec(ctx, `
		INSERT INTO engagement.processed_events (event_id)
		VALUES ($1)
		ON CONFLICT (event_id) DO NOTHING`, validated.EventID)
	if err != nil {
		return fmt.Errorf("record processed event: %w", err)
	}
	if tag.RowsAffected() == 0 {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit duplicate event: %w", err)
		}
		return nil
	}

	if err := applyProjection(ctx, tx, validated); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit applied event: %w", err)
	}
	return nil
}

func applyProjection(ctx context.Context, tx pgx.Tx, event events.Envelope) error {
	switch event.EventType {
	case events.UserRegistered:
		return nil
	case events.PostCreated:
		var payload events.PostCreatedPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode PostCreated payload: %w", err)
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO engagement.post_like_counters (post_id, like_count, updated_at)
			VALUES ($1, 0, $2)
			ON CONFLICT (post_id) DO NOTHING`, payload.PostID, event.OccurredAt)
		if err != nil {
			return fmt.Errorf("create post like counter: %w", err)
		}
		return nil
	case events.PostLiked:
		var payload events.PostLikedPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode PostLiked payload: %w", err)
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO engagement.post_like_counters (post_id, like_count, updated_at)
			VALUES ($1, 1, $2)
			ON CONFLICT (post_id) DO UPDATE
			SET like_count = engagement.post_like_counters.like_count + 1,
			    updated_at = EXCLUDED.updated_at`, payload.PostID, event.OccurredAt)
		if err != nil {
			return fmt.Errorf("increment post like counter: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unsupported event type %q", event.EventType)
	}
}

func (db *Postgres) PostLikeCount(ctx context.Context, postID int64) (int64, bool, error) {
	var count int64
	err := db.pool.QueryRow(ctx, `
		SELECT like_count
		FROM engagement.post_like_counters
		WHERE post_id = $1`, postID).Scan(&count)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read post like counter: %w", err)
	}
	return count, true, nil
}

func (db *Postgres) Health(ctx context.Context) error {
	if err := db.pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping PostgreSQL: %w", err)
	}
	return nil
}
