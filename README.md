# MicroBlog

Небольшой REST-сервис на Go для публикации постов и лайков. Пользователи,
посты и лайки хранятся в PostgreSQL 17.9 и сохраняются после перезапуска.
Приложение использует pgxpool, SQL-билдер Squirrel и миграции Goose.

## Возможности

- регистрация пользователей;
- создание и просмотр постов;
- асинхронная постановка лайка в очередь;
- транзакционная обработка лайков и ограничения PK/FK/CHECK в БД;
- годовое range-партиционирование постов и индексы BTREE;
- событийное логирование в stdout;
- профилирование через `net/http/pprof`.

## Структура

```text
MicroBlog/
├── cmd/
│   ├── main.go                 # HTTP-сервер, pool, миграции, shutdown
│   └── migrate/                # CLI для Goose up/down
├── db/
│   ├── migrations/             # встроенные SQL-миграции Goose
│   └── explain.sql             # планы запросов и partition pruning
├── deployments/                # настройка подключения pgAdmin
├── internal/
│   ├── handlers/               # HTTP, JSON, передача контекста запроса
│   ├── logger/                 # логгер событий на канале
│   ├── models/                 # User и Post; прежний JSON-контракт
│   ├── queue/                  # очередь и worker для лайков
│   ├── repository/             # pgxpool + Squirrel, транзакции
│   ├── service/                # нормализация и бизнес-логика
│   ├── syncutils/              # атомарный счётчик из предыдущего спринта
│   └── testutil/               # тестовая БД
├── docker-compose.yml
├── Dockerfile
└── Makefile
```

## Запуск через Docker Compose

Требуется Docker с Compose v2:

```bash
docker compose up --build -d
# либо make up
```

После готовности PostgreSQL приложение автоматически применяет миграции.
Сервисы:

- API: http://localhost:8080;
- PostgreSQL: localhost:5432, база/пользователь/пароль `microblog`;
- pgAdmin: http://localhost:5050, логин `admin@example.com`, пароль `admin`.

Это локальные настройки для разработки. В pgAdmin сервер `MicroBlog` уже
добавлен: адрес `db:5432`, пользователь `microblog`, пароль `microblog`.
Тома `postgres_data` и `pgadmin_data` сохраняют данные при остановке контейнеров.

```bash
docker compose logs -f app
docker compose restart app       # данные остаются в БД
docker compose down              # тома сохраняются
```

`docker compose down -v` удаляет тома вместе с данными.
Порты можно переопределить, например:

```bash
APP_PORT=18080 DB_PORT=15432 PGADMIN_PORT=15050 docker compose up --build -d
```

## Локальный запуск Go

Требуется Go 1.26 и работающий PostgreSQL:

```bash
docker compose up -d db
export DATABASE_URL='postgres://microblog:microblog@localhost:5432/microblog?sslmode=disable'
go run ./cmd
# либо make run (с локальным DATABASE_URL по умолчанию)
```

`DATABASE_URL` обязателен для запуска без Make. `HTTP_ADDR` по умолчанию
равен `:8080`. При `Ctrl+C` сервер завершает HTTP-запросы, затем обрабатывает
принятые задачи очереди, закрывает логгер и пул БД. Одна задача лайка имеет
таймаут 5 секунд. При заполненной очереди ожидание постановки прекращается
при отмене HTTP-запроса или его таймауте (10 секунд). Очередь остаётся в памяти: `202 Accepted` подтверждает приём
задачи, а сохранение лайка выполняется после COMMIT. При принудительном
завершении процесса ещё не обработанные задачи теряются.

## Миграции

```bash
make migrate-up
make migrate-down  # откатывает последнюю миграцию; 001 удаляет все три таблицы
# Для другой БД:
make migrate-up DATABASE_URL='postgres://user:password@localhost:5432/db?sslmode=disable'
```

CLI находится в `cmd/migrate`, миграции встроены в оба бинарника. Отдельная
установка Goose не нужна. Файл `001_init.sql` содержит секции `-- +goose Up`
и `-- +goose Down`: это формат Goose вместо пары файлов `.up.sql/.down.sql`.
В контейнере команды доступны как `docker compose exec app /app/migrate up`
и `docker compose exec app /app/migrate down`. Перед откатом остановите запросы
к приложению: после удаления таблиц API будет возвращать 500.

## Схема и партиции

`users.id` остаётся строковым именем пользователя, публичный `posts.id`
выдаётся общей identity-последовательностью PostgreSQL. Во всех таблицах есть
`created_at TIMESTAMPTZ NOT NULL DEFAULT now()`.

Посты разделены по `created_at` на UTC-годы `posts_2024`–`posts_2027`.
`posts_default` принимает даты вне этих диапазонов, поэтому после 2027 года
запись продолжит работать. Новые годовые партиции добавляются отдельной
миграцией; если в default уже есть строки за нужный год, перед созданием
партиции их нужно перенести.

PK постов — `(id, created_at)`: PostgreSQL требует включать ключ
партиционирования в уникальное ограничение родительской таблицы.
`likes` ссылается на пост составным FK `(post_id, post_created_at)`, а PK
`(post_id, post_created_at, user_id)` запрещает повторный лайк. Индекс этого
PK покрывает поиск по `post_id`; отдельные BTREE-индексы созданы для
`posts.user_id` и `likes.user_id`. Пользователь и текст не могут быть пустыми.

Worker выполняет проверку пользователя и поста, вставку лайка и чтение
результата в одной транзакции. Событие публикуется после успешного COMMIT.
При ошибке транзакция откатывается, worker пишет причину в лог.

```bash
make explain
```

Команда выполняет `db/explain.sql`: `EXPLAIN (ANALYZE, BUFFERS)` для поиска
постов автора за год и поиска лайков по посту/пользователю. На маленькой
таблице PostgreSQL может предпочесть последовательное сканирование.
Условие по `created_at` позволяет исключить ненужные партиции; запрос
только по `id` проверяет индексы всех партиций.

## API

| Метод | Путь | Успешный ответ | Назначение |
| --- | --- | --- | --- |
| `POST` | `/register` | `201 Created` | Регистрирует пользователя |
| `POST` | `/posts` | `201 Created` | Создаёт пост от зарегистрированного пользователя |
| `GET` | `/posts` | `200 OK` | Возвращает ленту постов |
| `POST` | `/posts/{id}/like` | `202 Accepted` | Ставит задачу лайка в фоновую очередь |

Все тела запросов и ответов используют JSON.

### Регистрация

```bash
curl -i -X POST http://localhost:8080/register \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice"}'
```

Ответ `201 Created`:

```json
{"id":"alice","username":"alice"}
```

Пустое или уже занятое имя возвращает `400 Bad Request`.

### Создание поста

```bash
curl -i -X POST http://localhost:8080/posts \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","text":"Привет, мир!"}'
```

Ответ `201 Created`:

```json
{
  "id": 1,
  "author": {"id": "alice", "username": "alice"},
  "text": "Привет, мир!",
  "likes": []
}
```

Если пользователь не зарегистрирован, сервер вернёт `404 Not Found`.

### Лента

```bash
curl -i http://localhost:8080/posts
```

Ответ `200 OK` содержит массив постов. Для нового поста поле `likes` всегда
имеет вид пустого массива `[]`, а не `null`.

### Асинхронный лайк

```bash
curl -i -X POST http://localhost:8080/posts/1/like \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice"}'
```

Ответ `202 Accepted` означает, что задача принята очередью:

```json
{"status":"like accepted"}
```

Лайк применяется worker-горутиной, поэтому затем повторно запросите ленту:

```bash
curl http://localhost:8080/posts
```

```json
[
  {
    "id": 1,
    "author": {"id": "alice", "username": "alice"},
    "text": "Привет, мир!",
    "likes": ["alice"]
  }
]
```

Один пользователь может поставить одному посту только один лайк. Запрос на
несуществующий пост или от незарегистрированного пользователя вернёт
`404 Not Found`.

## Логирование и профилирование

В терминале сервера выводятся события регистрации, создания постов и успешно
обработанных лайков. Например:

```text
microblog: user registered: alice
microblog: post created: Привет, мир!
microblog: post liked: alice
```

Профили Go доступны по адресу:

```text
http://localhost:8080/debug/pprof/
```

## Проверки

Unit-тесты не требуют Docker, используют тестовую БД из `internal/testutil`:

```bash
go test ./...
go test -race ./...
go vet ./...
```

Интеграционные тесты через testcontainers-go поднимают временные контейнеры
`postgres:17.9-alpine`; Docker должен быть запущен. База из Compose не меняется.

```bash
make test-integration
# либо:
go test -tags=integration -count=1 ./...
```

Проверяются PK/FK/CHECK, конкурентная регистрация и повторные лайки, rollback
при ошибке COMMIT, сохранение данных и ID после переподключения, годовые
партиции и default, EXPLAIN, Goose down/up и отмена запросов. HTTP-тест
проходит регистрацию → пост → асинхронный лайк → закрытие приложения → новое
приложение с новым пулом → чтение сохранённого лайка. При отказе БД API
возвращает 500 без внутренних SQL-ошибок в ответе.

Бенчмарк слоя бизнес-логики на тестовой БД (не измеряет PostgreSQL):

```bash
go test -bench=BenchmarkCreatePost -benchmem ./internal/service
```
