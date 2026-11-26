package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"MicroBlog/db/migrations"
	"MicroBlog/internal/database"
	"MicroBlog/services/api/internal/handlers"
	"MicroBlog/services/api/internal/kafka"
	"MicroBlog/services/api/internal/outbox"
	"MicroBlog/services/api/internal/repository"
	"MicroBlog/services/api/internal/service"

	"github.com/jackc/pgx/v5/pgxpool"
)

type application struct {
	Handler http.Handler
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err := run(ctx)
	if err != nil {
		slog.Error("API stopped", "service", "api", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
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
	app := newApplication(db)
	brokers := kafkaBrokers()
	relayConfig, err := relayConfigFromEnv()
	if err != nil {
		return err
	}
	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	relay := outbox.NewRelay(db, producer, logger, relayConfig)
	relayContext, stopRelay := context.WithCancel(ctx)
	relayDone := make(chan struct{})
	var relayErr error
	go func() {
		relayErr = relay.Run(relayContext)
		if relayErr != nil {
			logger.Error("outbox relay stopped", "error", relayErr)
		}
		close(relayDone)
	}()
	relayStopped := false
	defer func() {
		stopRelay()
		if !relayStopped {
			select {
			case <-relayDone:
			case <-time.After(5 * time.Second):
				logger.Error("outbox relay did not stop before timeout")
			}
		}
		if err := producer.Close(); err != nil {
			logger.Error("close Kafka producer failed", "error", err)
		}
	}()

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
	logger.InfoContext(ctx, "API HTTP server is running", "service", "api", "address", address)

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
	stopRelay()
	select {
	case <-relayDone:
		relayStopped = true
		if relayErr != nil {
			return fmt.Errorf("outbox relay: %w", relayErr)
		}
	case <-time.After(5 * time.Second):
		return errors.New("outbox relay did not stop before timeout")
	}
	return nil
}

func kafkaBrokers() []string {
	value := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if value == "" {
		value = "localhost:9092"
	}
	return strings.Split(value, ",")
}

func relayConfigFromEnv() (outbox.Config, error) {
	topic := strings.TrimSpace(os.Getenv("KAFKA_TOPIC"))
	if topic == "" {
		topic = outbox.TopicName
	}
	initialBackoff, err := positiveDurationEnv("KAFKA_RETRY_INITIAL", time.Second)
	if err != nil {
		return outbox.Config{}, err
	}
	maxBackoff, err := positiveDurationEnv("KAFKA_RETRY_MAX", 30*time.Second)
	if err != nil {
		return outbox.Config{}, err
	}
	if maxBackoff < initialBackoff {
		return outbox.Config{}, errors.New("KAFKA_RETRY_MAX must not be shorter than KAFKA_RETRY_INITIAL")
	}
	pollInterval, err := positiveDurationEnv("OUTBOX_POLL_INTERVAL", 250*time.Millisecond)
	if err != nil {
		return outbox.Config{}, err
	}
	backlogInterval, err := positiveDurationEnv("OUTBOX_BACKLOG_LOG_INTERVAL", 30*time.Second)
	if err != nil {
		return outbox.Config{}, err
	}
	return outbox.Config{
		Topic:            topic,
		InitialBackoff:   initialBackoff,
		MaxBackoff:       maxBackoff,
		IdlePollInterval: pollInterval,
		BacklogInterval:  backlogInterval,
	}, nil
}

func positiveDurationEnv(name string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 0, fmt.Errorf("%s must be a positive Go duration", name)
	}
	return duration, nil
}

// prepareDatabase ограничивает подключение и миграции тридцатью секундами.
// После возврата контекст отменяется, но готовый пул остаётся рабочим.
func prepareDatabase(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	startupContext, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	pool, err := database.OpenPool(startupContext, dsn)
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

func newApplication(db repository.DB) *application {
	appService := service.New(db)
	api := handlers.New(appService)

	apiWithTimeout := http.TimeoutHandler(api, 10*time.Second, "request timed out")

	app := &application{}
	mux := http.NewServeMux()
	mux.Handle("/debug/pprof/", http.DefaultServeMux)
	mux.Handle("/", apiWithTimeout)
	app.Handler = mux

	return app
}
