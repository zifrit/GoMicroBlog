-- +goose Up
CREATE SCHEMA api;
CREATE SCHEMA engagement;

ALTER TABLE public.users SET SCHEMA api;
ALTER TABLE public.posts SET SCHEMA api;
ALTER TABLE public.posts_2024 SET SCHEMA api;
ALTER TABLE public.posts_2025 SET SCHEMA api;
ALTER TABLE public.posts_2026 SET SCHEMA api;
ALTER TABLE public.posts_2027 SET SCHEMA api;
ALTER TABLE public.posts_default SET SCHEMA api;
ALTER TABLE public.likes SET SCHEMA api;

CREATE TABLE api.outbox_events (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    message_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT
);
CREATE INDEX outbox_events_pending_idx ON api.outbox_events (id) WHERE published_at IS NULL;

CREATE TABLE engagement.processed_events (
    event_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE engagement.post_like_counters (
    post_id BIGINT PRIMARY KEY,
    like_count BIGINT NOT NULL CHECK (like_count >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
LOCK TABLE api.outbox_events IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM api.outbox_events WHERE published_at IS NULL) THEN
        RAISE EXCEPTION 'cannot roll back sprint 4 schemas while unpublished outbox events exist';
    END IF;
END
$$;
-- +goose StatementEnd

DROP TABLE engagement.post_like_counters;
DROP TABLE engagement.processed_events;
DROP SCHEMA engagement;

DROP TABLE api.outbox_events;
ALTER TABLE api.likes SET SCHEMA public;
ALTER TABLE api.posts SET SCHEMA public;
ALTER TABLE api.posts_2024 SET SCHEMA public;
ALTER TABLE api.posts_2025 SET SCHEMA public;
ALTER TABLE api.posts_2026 SET SCHEMA public;
ALTER TABLE api.posts_2027 SET SCHEMA public;
ALTER TABLE api.posts_default SET SCHEMA public;
ALTER TABLE api.users SET SCHEMA public;
DROP SCHEMA api;
