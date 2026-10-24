package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"MicroBlog/db/migrations"
	"MicroBlog/internal/repository"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 2 {
		return fmt.Errorf("usage: migrate up|down")
	}
	command := os.Args[1]
	if command != "up" && command != "down" {
		return fmt.Errorf("usage: migrate up|down")
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, err := repository.Open(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	if command == "down" {
		return migrations.Down(ctx, pool)
	}
	return migrations.Up(ctx, pool)
}
