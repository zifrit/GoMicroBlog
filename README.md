# MicroBlog

Небольшой REST-сервис на Go для публикации постов и лайков. В Sprint 4 приложение разделено на два HTTP-сервиса: `api` принимает пользовательские запросы, а `engagement` строит проекцию счётчиков лайков из событий Kafka.

Стек: Go 1.26, PostgreSQL 17.9, Apache Kafka 4.4.0 в KRaft-режиме, Goose, pgx и franz-go.

## Архитектура

- `api` обслуживает регистрацию, посты и лайки. Он записывает изменение данных и событие в `api.outbox_events` одной транзакцией PostgreSQL.
- Фоновый outbox relay публикует события в Kafka после COMMIT. Он повторяет отправку с exponential backoff и не удаляет событие после ошибки.
- `engagement` читает Kafka и в одной транзакции записывает ID обработанного события и обновляет `engagement.post_like_counters`. Повторная доставка не увеличивает счётчик повторно.
- Одна база данных содержит схемы `api` и `engagement`. Kafka topic по умолчанию — `microblog.events`, с тремя partitions и replication factor 1.
- Для `microblog.events` задан `retention.ms=-1`: Kafka не удаляет события по возрасту, пока сохраняется её volume. Это исключает потерю ещё не обработанных событий из-за срока хранения, но журнал растёт без ограничения; следите за свободным местом.
- Публикация и потребление работают с гарантией at-least-once. Сбой после Kafka ACK, но до отметки outbox может дать повторную доставку; `engagement.processed_events` устраняет повторный эффект.

```text
REST client → api → PostgreSQL api schema → outbox relay → Kafka
                                                        ↓
REST client ← engagement REST ← PostgreSQL projection ← consumer
```

## Структура

```text
MicroBlog/
├── cmd/migrate/                  # CLI для Goose up/down
├── db/migrations/                # миграции PostgreSQL
├── internal/database/            # общий конструктор пула PostgreSQL
├── internal/events/              # общий контракт событий
├── services/api/                 # REST API, repository, outbox relay, Kafka producer
├── services/engagement/          # REST статистика, Kafka consumer, проекция
├── Dockerfile.api
├── Dockerfile.engagement
├── docker-compose.yml
└── Makefile
```

## Запуск через Docker Compose

Требуется Docker с Compose v2. Для локальной разработки можно скопировать шаблон окружения:

```bash
cp .env.example .env
docker compose up --build -d --remove-orphans
# либо make up
```

Стандартный Compose запускает четыре сервиса: `api`, `engagement`, `db` и один Kafka broker/controller в KRaft-режиме. PostgreSQL и Kafka сохраняют состояние в Docker volumes. При старте `api` применяет миграции Goose. Его health check проверяет PostgreSQL; доступность Kafka не блокирует HTTP API. Сервис `engagement` начинает запуск после готовности `api` и PostgreSQL, но REST статистики работает независимо от Kafka.

Адреса по умолчанию:

- API: http://localhost:8080;
- статистика: http://localhost:8081;
- PostgreSQL: `localhost:5432`;
- Kafka для локальных Go-процессов: `localhost:9092`.

pgAdmin не запускается в стандартной конфигурации. Запустить его можно отдельно:

```bash
docker compose --profile tools up --build -d
```

pgAdmin будет доступен по адресу http://localhost:5050. Учётные данные берутся из `PGADMIN_DEFAULT_EMAIL` и `PGADMIN_DEFAULT_PASSWORD` в `.env`. Добавьте сервер `MicroBlog` вручную: host `db`, port `5432`, а имя базы, пользователь и пароль — значения `POSTGRES_DB`, `POSTGRES_USER` и `POSTGRES_PASSWORD`.

### Обновление с Sprint 3

Сначала остановите старый API из Sprint 3 checkout, пока там ещё доступна старая Compose-конфигурация. Команда ждёт завершения принятой очереди лайков. Затем остановите остальные контейнеры без удаления named volumes:

```bash
docker compose stop --timeout 720 app
docker compose down
```

После обновления файлов Sprint 4 скопируйте `.env.example` в `.env` и выполните `docker compose up --build -d --remove-orphans`. Сервис `db` подключится к существующему `postgres_data`; миграция перенесёт таблицы в схему `api`. Шаблон сохраняет прежние локальные реквизиты PostgreSQL (`microblog`), потому что изменение `POSTGRES_PASSWORD` в окружении не меняет пароль роли в уже созданном volume. Для другого существующего пароля задайте его и в `POSTGRES_PASSWORD`, и в `DATABASE_URL`.

```bash
docker compose ps
docker compose logs -f api engagement kafka
docker compose down       # named volumes сохраняются
docker compose down -v    # удаляет БД и Kafka вместе с volumes
```

Порты можно изменить в `.env`: `API_PORT`, `ENGAGEMENT_PORT`, `DB_PORT`, `KAFKA_PORT` и `PGADMIN_PORT`. При смене `KAFKA_PORT` локальные Go-процессы подключаются через `KAFKA_BROKERS=localhost:<новый порт>`. При смене пароля PostgreSQL синхронно обновите `POSTGRES_PASSWORD` и пароль в `DATABASE_URL`. Значения `.env.example` предназначены только для локальной разработки.

## Локальный запуск Go

Требуются Go 1.26 и Docker с Compose. Поднимите PostgreSQL и Kafka:

```bash
docker compose up -d db kafka
make run-api
```

В другом терминале запустите consumer и REST-сервис статистики:

```bash
make run-engagement
```

Makefile по умолчанию подключается к `localhost:5432` и Kafka `localhost:9092`. Для других адресов передайте переменные при запуске:

```bash
make run-api DATABASE_URL='postgres://user:password@localhost:5432/microblog?sslmode=disable' KAFKA_BROKERS=localhost:9092
make run-engagement DATABASE_URL='postgres://user:password@localhost:5432/microblog?sslmode=disable' KAFKA_BROKERS=localhost:9092
```

Основные настройки: `DATABASE_URL`, `HTTP_ADDR`, `KAFKA_BROKERS`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID`, `OUTBOX_POLL_INTERVAL`, `KAFKA_RETRY_INITIAL`, `KAFKA_RETRY_MAX` и `OUTBOX_BACKLOG_LOG_INTERVAL`. Длительности задаются в формате Go, например `250ms`, `1s` или `30s`. Backlog outbox доступен в структурированных логах relay как `pending_count` и `oldest_pending_age_seconds`; REST endpoint для него не добавлен.

## REST API

Все тела запросов и ответов используют JSON.

| Метод | Путь | Успешный ответ | Назначение |
| --- | --- | --- | --- |
| `POST` | `http://localhost:8080/register` | `201 Created` | Регистрирует пользователя |
| `POST` | `http://localhost:8080/posts` | `201 Created` | Создаёт пост |
| `GET` | `http://localhost:8080/posts` | `200 OK` | Возвращает ленту |
| `POST` | `http://localhost:8080/posts/{id}/like` | `202 Accepted` | Фиксирует лайк и событие в outbox |
| `GET` | `http://localhost:8080/healthz` | `200 OK` / `503` | Проверяет PostgreSQL для API |
| `GET` | `http://localhost:8081/stats/posts/{id}` | `200 OK` / `404` | Возвращает проекцию счётчика лайков |
| `GET` | `http://localhost:8081/healthz` | `200 OK` / `503` | Проверяет PostgreSQL для `engagement` |

### Регистрация и пост

```bash
curl -i -X POST http://localhost:8080/register \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice"}'

curl -i -X POST http://localhost:8080/posts \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","text":"Привет, мир!"}'
```

Регистрация отвечает `201 Created`, например `{"id":"alice","username":"alice"}`. Создание поста также отвечает `201 Created`. Неизвестный пользователь при создании поста даёт `404 Not Found`.

### Лайк и ответ `202 Accepted`

```bash
curl -i -X POST http://localhost:8080/posts/1/like \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice"}'
```

`202 Accepted` возвращается после COMMIT лайка и события `PostLiked` в одной транзакции PostgreSQL. С этого момента запрос надёжно принят, даже если Kafka временно недоступна. Relay повторяет публикацию после восстановления брокера. Статистика обновляется позже, после обработки события сервисом `engagement`.

Повторный лайк того же пользователя возвращает такой же `202 Accepted`, но не меняет лайки и не создаёт новое событие. Лента содержит не более одного лайка пользователя на пост. Несуществующий пост или пользователь возвращает `404 Not Found`.

```bash
curl -i http://localhost:8080/posts
curl -i http://localhost:8081/stats/posts/1
```

Ответ статистики имеет форму `{"post_id":1,"like_count":1}`. После `PostCreated` счётчик равен нулю и endpoint отвечает `200`. Если проекции ещё нет, endpoint отвечает `404`.

### Посты и лайки, существовавшие до Sprint 4

Миграция переносит текущие `users`, `posts` и `likes` из `public` в схему `api`, сохраняя строки. Проекция `engagement` начинается пустой и не делает backfill старых лайков. Поэтому для старого поста `/stats/posts/{id}` отвечает `404`, пока после миграции не появится событие для него. Первый новый лайк старого поста создаёт счётчик со значением `1`; прежние лайки в него не входят. Новые посты получают нулевую строку проекции через `PostCreated`.

## Миграции и схема БД

```bash
make migrate-up
make migrate-down
```

`api` применяет миграции при запуске. Команды Makefile позволяют управлять миграциями отдельно. Sprint 4 migration переносит таблицы и существующие данные в `api`, а также создаёт outbox и таблицы проекции в `engagement`. Down-миграция отказывается выполняться, если в outbox остались неопубликованные события. Перед откатом остановите новые записи и дождитесь пустого outbox. Откат `001_init.sql` удаляет исходные таблицы и их данные.

`posts` использует годовые range partitions `posts_2024`–`posts_2027` и `posts_default`. PK постов — `(id, created_at)`, а `likes` ссылается на составной ключ. PK лайка `(post_id, post_created_at, user_id)` запрещает повторный лайк. PostgreSQL требует включать ключ партиционирования в уникальный ключ родительской таблицы.

Запросы для проверки планов выполняются в schema `api`:

```bash
make explain
```

## Проверки

Unit-тесты, race detector и `go vet`:

```bash
make test
make test-race
make vet
```

Полный набор integration-тестов поднимает временные контейнеры PostgreSQL и Kafka через testcontainers-go. Требуется запущенный Docker:

```bash
make test-integration
# эквивалентно:
go test -tags=integration -count=1 ./...
```

Конфигурация обоих Compose-профилей и Go-сборка:

```bash
make config
make build
```
