package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"MicroBlog/internal/database"
	"MicroBlog/services/engagement/internal/consumer"
	"MicroBlog/services/engagement/internal/handlers"
	"MicroBlog/services/engagement/internal/repository"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		logger.Error("engagement service stopped", "service", "engagement", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dsn == "" {
		return errors.New("DATABASE_URL is required")
	}
	pool, err := database.OpenPool(ctx, dsn)
	if err != nil {
		return fmt.Errorf("open PostgreSQL: %w", err)
	}
	defer pool.Close()

	initialBackoff, err := durationEnv("KAFKA_RETRY_INITIAL", time.Second)
	if err != nil {
		return err
	}
	maxBackoff, err := durationEnv("KAFKA_RETRY_MAX", 30*time.Second)
	if err != nil {
		return err
	}
	topic := strings.TrimSpace(os.Getenv("KAFKA_TOPIC"))
	groupID := strings.TrimSpace(os.Getenv("KAFKA_GROUP_ID"))
	eventConsumer, err := consumer.New(kafkaBrokers(), repository.New(pool), logger, consumer.Config{
		Topic:          topic,
		GroupID:        groupID,
		InitialBackoff: initialBackoff,
		MaxBackoff:     maxBackoff,
	})
	if err != nil {
		return err
	}
	consumerContext, stopConsumer := context.WithCancel(ctx)
	consumerDone := make(chan struct{})
	var consumerErr error
	go func() {
		consumerErr = eventConsumer.Run(consumerContext)
		if consumerErr != nil {
			logger.Error("Kafka consumer stopped", "service", "engagement", "topic", topic, "error", consumerErr)
		}
		close(consumerDone)
	}()
	consumerStopped := false
	defer func() {
		stopConsumer()
		if !consumerStopped {
			select {
			case <-consumerDone:
			case <-time.After(5 * time.Second):
				logger.Error("Kafka consumer did not stop before timeout", "service", "engagement")
			}
		}
		eventConsumer.Close()
	}()

	address := strings.TrimSpace(os.Getenv("HTTP_ADDR"))
	if address == "" {
		address = ":8081"
	}
	server := &http.Server{
		Addr:              address,
		Handler:           http.TimeoutHandler(handlers.New(repository.New(pool)), 10*time.Second, "request timed out"),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- server.ListenAndServe()
	}()
	logger.InfoContext(ctx, "engagement HTTP server is running", "service", "engagement", "address", address)

	select {
	case err := <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("engagement HTTP server: %w", err)
	case <-consumerDone:
		consumerStopped = true
		if consumerErr != nil {
			return fmt.Errorf("Kafka consumer: %w", consumerErr)
		}
		return errors.New("Kafka consumer stopped before shutdown")
	case <-ctx.Done():
	}

	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := server.Shutdown(shutdownContext); err != nil {
		_ = server.Close()
		return fmt.Errorf("engagement HTTP shutdown: %w", err)
	}
	stopConsumer()
	select {
	case <-consumerDone:
		consumerStopped = true
		if consumerErr != nil {
			return fmt.Errorf("Kafka consumer shutdown: %w", consumerErr)
		}
	case <-time.After(5 * time.Second):
		return errors.New("Kafka consumer did not stop before timeout")
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

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
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
