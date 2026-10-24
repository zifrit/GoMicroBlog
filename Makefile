DATABASE_URL ?= postgres://microblog:microblog@localhost:5432/microblog?sslmode=disable
export DATABASE_URL

.PHONY: up down run migrate-up migrate-down test test-race test-integration explain

up:
	docker compose up --build -d

down:
	docker compose down

run:
	go run ./cmd

migrate-up:
	go run ./cmd/migrate up

migrate-down:
	go run ./cmd/migrate down

test:
	go test ./...

test-race:
	go test -race ./...

test-integration:
	go test -tags=integration -count=1 ./...

explain:
	docker compose exec -T db psql -U microblog -d microblog -v ON_ERROR_STOP=1 < db/explain.sql
