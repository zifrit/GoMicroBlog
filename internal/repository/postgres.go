package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"MicroBlog/internal/models"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgreSQL возвращает эти коды при повторной записи и нарушении внешнего ключа.
const (
	uniqueViolation     = "23505"
	foreignKeyViolation = "23503"
)

var sqlBuilder = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

// Postgres выполняет операции с БД через пул соединений PostgreSQL.
type Postgres struct {
	pool *pgxpool.Pool
}

// Open создаёт пул и проверяет, что PostgreSQL отвечает.
// Контекст ограничивает подготовку пула; готовый пул работает до вызова Close.
func Open(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("database configuration: %w", err)
	}
	config.ConnConfig.ConnectTimeout = 5 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("database pool: %w", err)
	}

	err = pool.Ping(ctx)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("database ping: %w", err)
	}
	return pool, nil
}

// New создаёт объект для запросов к БД.
func New(pool *pgxpool.Pool) *Postgres {
	return &Postgres{
		pool: pool,
	}
}

// Register сохраняет пользователя. Повторное имя запрещено ограничением БД.
func (db *Postgres) Register(ctx context.Context, username string) (*models.User, error) {
	query, args, err := sqlBuilder.
		Insert("users").
		Columns("id", "username").
		Values(username, username).
		Suffix("RETURNING id, username").
		ToSql()
	if err != nil {
		return nil, err
	}

	user := &models.User{}
	row := db.pool.QueryRow(ctx, query, args...)
	err = row.Scan(&user.ID, &user.Username)
	if err != nil {
		if hasPostgresErrorCode(err, uniqueViolation) {
			return nil, ErrUserExists
		}
		return nil, fmt.Errorf("register user: %w", err)
	}
	return user, nil
}

// CreatePost сохраняет пост и получает его номер из БД.
func (db *Postgres) CreatePost(ctx context.Context, username, text string) (*models.Post, error) {
	query, args, err := sqlBuilder.
		Insert("posts").
		Columns("user_id", "text").
		Values(username, text).
		Suffix("RETURNING id").
		ToSql()
	if err != nil {
		return nil, err
	}

	var postID int
	row := db.pool.QueryRow(ctx, query, args...)
	err = row.Scan(&postID)
	if err != nil {
		if hasPostgresErrorCode(err, foreignKeyViolation) {
			return nil, ErrUserNotFound
		}
		return nil, fmt.Errorf("create post: %w", err)
	}

	author := &models.User{
		ID:       username,
		Username: username,
	}
	post := &models.Post{
		ID:     postID,
		Author: author,
		Text:   text,
		Likes:  []string{},
	}
	return post, nil
}

// LEFT JOIN оставляет в результате и посты без лайков.
// Если лайков несколько, БД возвращает несколько строк одного поста.
func postsQuery() sq.SelectBuilder {
	return sqlBuilder.
		Select("p.id", "p.created_at", "u.id", "u.username", "p.text", "l.user_id").
		From("posts p").
		Join("users u ON u.id = p.user_id").
		LeftJoin("likes l ON l.post_id = p.id AND l.post_created_at = p.created_at").
		OrderBy("p.id", "p.created_at", "l.created_at", "l.user_id")
}

// readPosts считывает основные поля сразу в post и собирает его лайки из строк JOIN.
func readPosts(rows pgx.Rows) ([]*models.Post, error) {
	defer rows.Close()

	posts := make([]*models.Post, 0)
	var currentPost *models.Post
	var currentCreatedAt time.Time

	for rows.Next() {
		post := &models.Post{
			Author: &models.User{},
			Likes:  []string{},
		}
		var createdAt time.Time
		var likedBy *string

		err := rows.Scan(
			&post.ID,
			&createdAt,
			&post.Author.ID,
			&post.Author.Username,
			&post.Text,
			&likedBy,
		)
		if err != nil {
			return nil, fmt.Errorf("scan post: %w", err)
		}

		// ORDER BY располагает строки одного поста рядом.
		// Добавляем пост один раз, даже если у него несколько лайков.
		if currentPost == nil || currentPost.ID != post.ID || !currentCreatedAt.Equal(createdAt) {
			currentPost = post
			currentCreatedAt = createdAt
			posts = append(posts, currentPost)
		}

		// При LEFT JOIN у поста без лайков likedBy равен nil.
		if likedBy != nil {
			currentPost.Likes = append(currentPost.Likes, *likedBy)
		}
	}

	err := rows.Err()
	if err != nil {
		return nil, fmt.Errorf("read posts: %w", err)
	}
	return posts, nil
}

// ListPosts получает ленту одним запросом к БД.
func (db *Postgres) ListPosts(ctx context.Context) ([]*models.Post, error) {
	query, args, err := postsQuery().ToSql()
	if err != nil {
		return nil, err
	}

	rows, err := db.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list posts: %w", err)
	}
	return readPosts(rows)
}

// И пул, и транзакция умеют выполнять QueryRow.
// Поэтому проверки пользователя и поста работают в обоих случаях.
type rowReader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func checkUserExists(ctx context.Context, db rowReader, username string) error {
	query, args, err := sqlBuilder.
		Select("id").
		From("users").
		Where(sq.Eq{"id": username}).
		ToSql()
	if err != nil {
		return err
	}

	var userID string
	row := db.QueryRow(ctx, query, args...)
	err = row.Scan(&userID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrUserNotFound
	}
	if err != nil {
		return fmt.Errorf("find user: %w", err)
	}
	return nil
}

func findPostCreatedAt(ctx context.Context, db rowReader, postID int) (time.Time, error) {
	query, args, err := sqlBuilder.
		Select("created_at").
		From("posts").
		Where(sq.Eq{"id": postID}).
		ToSql()
	if err != nil {
		return time.Time{}, err
	}

	var createdAt time.Time
	row := db.QueryRow(ctx, query, args...)
	err = row.Scan(&createdAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, ErrPostNotFound
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("find post: %w", err)
	}
	return createdAt, nil
}

// ValidateLike проверяет пользователя и пост перед постановкой задачи в очередь.
func (db *Postgres) ValidateLike(ctx context.Context, postID int, username string) error {
	err := checkUserExists(ctx, db.pool, username)
	if err != nil {
		return err
	}

	_, err = findPostCreatedAt(ctx, db.pool, postID)
	return err
}

// LikePost выполняет запись лайка в одной транзакции.
func (db *Postgres) LikePost(ctx context.Context, postID int, username string) (*models.Post, error) {
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin like: %w", err)
	}
	defer tx.Rollback(ctx)

	// Сначала проверяем пользователя и находим составной ключ поста.
	err = checkUserExists(ctx, tx, username)
	if err != nil {
		return nil, err
	}
	createdAt, err := findPostCreatedAt(ctx, tx, postID)
	if err != nil {
		return nil, err
	}

	query, args, err := sqlBuilder.
		Insert("likes").
		Columns("post_id", "post_created_at", "user_id").
		Values(postID, createdAt, username).
		ToSql()
	if err != nil {
		return nil, err
	}

	_, err = tx.Exec(ctx, query, args...)
	if err != nil {
		if hasPostgresErrorCode(err, uniqueViolation) {
			return nil, ErrAlreadyLiked
		}
		return nil, fmt.Errorf("insert like: %w", err)
	}

	// Читаем пост через ту же транзакцию, чтобы видеть добавленный лайк.
	query, args, err = postsQuery().
		Where(sq.Eq{"p.id": postID, "p.created_at": createdAt}).
		ToSql()
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("read liked post: %w", err)
	}
	posts, err := readPosts(rows)
	if err != nil {
		return nil, err
	}
	if len(posts) == 0 {
		return nil, ErrPostNotFound
	}

	// Возвращаем результат только после сохранения транзакции в БД.
	err = tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("commit like: %w", err)
	}
	return posts[0], nil
}

func hasPostgresErrorCode(err error, code string) bool {
	var databaseError *pgconn.PgError
	if !errors.As(err, &databaseError) {
		return false
	}
	return databaseError.Code == code
}
