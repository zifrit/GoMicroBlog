// Package migrations содержит встроенные миграции Goose для приложения и CLI.
package migrations

import (
	"context"
	"embed"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

//go:embed *.sql
var files embed.FS

// Up применяет миграции, которых ещё нет в БД.
func Up(ctx context.Context, pool *pgxpool.Pool) error {
	// Goose работает через database/sql; используем для него готовый пул pgx.
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()

	migrationProvider, err := goose.NewProvider(goose.DialectPostgres, db, files)
	if err != nil {
		return err
	}
	_, err = migrationProvider.Up(ctx)
	return err
}

// Down откатывает последнюю миграцию и удаляет созданные ею данные.
func Down(ctx context.Context, pool *pgxpool.Pool) error {
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()

	migrationProvider, err := goose.NewProvider(goose.DialectPostgres, db, files)
	if err != nil {
		return err
	}
	_, err = migrationProvider.Down(ctx)
	return err
}
