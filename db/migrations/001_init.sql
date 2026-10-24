-- +goose Up
CREATE TABLE users (
    id TEXT PRIMARY KEY CHECK (btrim(id) <> ''),
    username TEXT NOT NULL UNIQUE CHECK (btrim(username) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (id = username)
);

CREATE TABLE posts (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    user_id TEXT NOT NULL REFERENCES users(id),
    text TEXT NOT NULL CHECK (btrim(text) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE posts_2024 PARTITION OF posts FOR VALUES FROM ('2024-01-01 00:00:00+00') TO ('2025-01-01 00:00:00+00');
CREATE TABLE posts_2025 PARTITION OF posts FOR VALUES FROM ('2025-01-01 00:00:00+00') TO ('2026-01-01 00:00:00+00');
CREATE TABLE posts_2026 PARTITION OF posts FOR VALUES FROM ('2026-01-01 00:00:00+00') TO ('2027-01-01 00:00:00+00');
CREATE TABLE posts_2027 PARTITION OF posts FOR VALUES FROM ('2027-01-01 00:00:00+00') TO ('2028-01-01 00:00:00+00');
CREATE TABLE posts_default PARTITION OF posts DEFAULT;
CREATE INDEX posts_user_id_idx ON posts USING btree (user_id);

CREATE TABLE likes (
    post_id BIGINT NOT NULL,
    post_created_at TIMESTAMPTZ NOT NULL,
    user_id TEXT NOT NULL REFERENCES users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (post_id, post_created_at, user_id),
    FOREIGN KEY (post_id, post_created_at) REFERENCES posts(id, created_at) ON DELETE CASCADE
);

CREATE INDEX likes_user_id_idx ON likes USING btree (user_id);

-- +goose Down
DROP TABLE likes;
DROP TABLE posts;
DROP TABLE users;
