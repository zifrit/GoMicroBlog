DATABASE_URL ?= postgres://microblog:microblog@localhost:5432/microblog?sslmode=disable
HTTP_ADDR ?= :8080
ENGAGEMENT_HTTP_ADDR ?= :8081
KAFKA_BROKERS ?= localhost:9092
KAFKA_TOPIC ?= microblog.events
KAFKA_GROUP_ID ?= engagement-stats-v1
OUTBOX_POLL_INTERVAL ?= 250ms
KAFKA_RETRY_INITIAL ?= 1s
KAFKA_RETRY_MAX ?= 30s
OUTBOX_BACKLOG_LOG_INTERVAL ?= 30s

.PHONY: up up-tools down run-api run-engagement migrate-up migrate-down test test-race test-integration vet build config explain

up:
	docker compose up --build -d --remove-orphans

up-tools:
	docker compose --profile tools up --build -d --remove-orphans

down:
	docker compose down

run-api:
	DATABASE_URL='$(DATABASE_URL)' HTTP_ADDR='$(HTTP_ADDR)' KAFKA_BROKERS='$(KAFKA_BROKERS)' KAFKA_TOPIC='$(KAFKA_TOPIC)' KAFKA_RETRY_INITIAL='$(KAFKA_RETRY_INITIAL)' KAFKA_RETRY_MAX='$(KAFKA_RETRY_MAX)' OUTBOX_POLL_INTERVAL='$(OUTBOX_POLL_INTERVAL)' OUTBOX_BACKLOG_LOG_INTERVAL='$(OUTBOX_BACKLOG_LOG_INTERVAL)' go run ./services/api

run-engagement:
	DATABASE_URL='$(DATABASE_URL)' HTTP_ADDR='$(ENGAGEMENT_HTTP_ADDR)' KAFKA_BROKERS='$(KAFKA_BROKERS)' KAFKA_TOPIC='$(KAFKA_TOPIC)' KAFKA_GROUP_ID='$(KAFKA_GROUP_ID)' KAFKA_RETRY_INITIAL='$(KAFKA_RETRY_INITIAL)' KAFKA_RETRY_MAX='$(KAFKA_RETRY_MAX)' go run ./services/engagement

migrate-up:
	DATABASE_URL='$(DATABASE_URL)' go run ./cmd/migrate up

migrate-down:
	DATABASE_URL='$(DATABASE_URL)' go run ./cmd/migrate down

test:
	go test ./...

test-race:
	go test -race ./...

test-integration:
	go test -tags=integration -count=1 ./...

vet:
	go vet ./...

build:
	go build ./services/api ./services/engagement ./cmd/migrate

config:
	docker compose config --quiet
	docker compose --profile tools config --quiet

explain:
	docker compose exec -T db psql -U "$${POSTGRES_USER:-microblog}" -d "$${POSTGRES_DB:-microblog}" -v ON_ERROR_STOP=1 < db/explain.sql
