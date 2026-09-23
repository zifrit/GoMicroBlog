# MicroBlog

Небольшой REST-сервис на Go для публикации постов и лайков. Данные хранятся
в памяти процесса: после остановки сервера пользователи, посты и лайки будут
удалены.

## Возможности

- регистрация пользователей;
- создание и просмотр постов;
- асинхронная постановка лайка в очередь;
- потокобезопасная работа с данными;
- событийное логирование в stdout;
- профилирование через `net/http/pprof`.

## Структура

```text
MicroBlog/
├── cmd/
│   └── main.go                 # запуск HTTP-сервера и инициализация зависимостей
└── internal/
    ├── handlers/               # HTTP-маршруты, JSON и коды ответов
    ├── logger/                 # логгер событий на канале
    ├── models/                 # User и Post
    ├── queue/                  # очередь и worker для лайков
    ├── service/                # бизнес-логика и потокобезопасное хранилище
    └── syncutils/              # атомарный счётчик ID постов
```

## Запуск

Требуется Go. Из корня проекта выполните:

```bash
go run ./cmd
```

Сервер слушает `http://localhost:8080`. Остановить его можно сочетанием
`Ctrl+C`; сервер перестанет принимать новые запросы и завершит обработку уже
поставленных в очередь лайков.

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

Запустить все unit-тесты:

```bash
go test ./...
```

Проверить гонки данных:

```bash
go test -race ./...
```

Запустить benchmark создания постов:

```bash
go test -bench=BenchmarkCreatePost -benchmem ./internal/service
```
