package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"MicroBlog/db/migrations"
	"MicroBlog/internal/handlers"
	"MicroBlog/internal/logger"
	"MicroBlog/internal/queue"
	"MicroBlog/internal/repository"
	"MicroBlog/internal/service"

	"github.com/jackc/pgx/v5/pgxpool"
)

type application struct {
	Handler http.Handler
	service *service.Service
	likes   *queue.LikeQueue
	events  *logger.Logger
}

func (app *application) Close() {
	app.likes.Close()
	app.events.Close()
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err := run(ctx, os.Stdout)
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(ctx context.Context, output io.Writer) error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is required")
	}

	pool, err := prepareDatabase(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	db := repository.New(pool)
	app := newApplication(output, db)

	defer app.Close()

	address := os.Getenv("HTTP_ADDR")
	if address == "" {
		address = ":8080"
	}
	server := &http.Server{
		Addr:              address,
		Handler:           app.Handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Сервер работает отдельно, пока main ждёт остановку или ошибку запуска.
	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- server.ListenAndServe()
	}()
	log.Printf("microblog server is running on %s", address)

	select {
	case err := <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("HTTP server: %w", err)
	case <-ctx.Done():
		// Пришёл сигнал остановки: завершаем текущие HTTP-запросы.
	}

	shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = server.Shutdown(shutdownContext)
	if err != nil {
		_ = server.Close()
		return fmt.Errorf("HTTP shutdown: %w", err)
	}
	return nil
}

// prepareDatabase ограничивает подключение и миграции тридцатью секундами.
// После возврата контекст отменяется, но готовый пул остаётся рабочим.
func prepareDatabase(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	startupContext, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	pool, err := repository.Open(startupContext, dsn)
	if err != nil {
		return nil, err
	}

	err = migrations.Up(startupContext, pool)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate database: %w", err)
	}
	return pool, nil
}

func newApplication(output io.Writer, db repository.DB) *application {
	events := logger.New(output, 128)
	appService := service.New(db, events)

	app := &application{
		service: appService,
		events:  events,
	}
	app.likes = queue.NewLikeQueue(128, app.processLike, app.logLikeError)

	api := handlers.New(appService, app.likes)

	apiWithTimeout := http.TimeoutHandler(api, 10*time.Second, "request timed out")

	mux := http.NewServeMux()
	mux.Handle("/debug/pprof/", http.DefaultServeMux)
	mux.Handle("/", apiWithTimeout)
	app.Handler = mux

	return app
}

// Фоновая задача продолжает работу после ответа HTTP, поэтому контекст у неё свой.
func (app *application) processLike(job queue.LikeJob) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := app.service.LikePost(ctx, job.PostID, job.Username)
	return err
}

func (app *application) logLikeError(job queue.LikeJob, err error) {
	message := fmt.Sprintf("like failed: post_id=%d username=%s error=%v", job.PostID, job.Username, err)
	app.events.Publish(message)
}
