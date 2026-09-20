package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"MicroBlog/internal/handlers"
	"MicroBlog/internal/logger"
	"MicroBlog/internal/queue"
	"MicroBlog/internal/service"
)

type application struct {
	Handler http.Handler
	close   func()
}

func (a *application) Close() {
	a.close()
}

func main() {
	app := newApplication(os.Stdout)
	defer app.Close()
	server := &http.Server{Addr: ":8080", Handler: app.Handler}

	log.Println("microblog server is running on http://localhost:8080")
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("server error: %v", err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	<-signals

	shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil {
		log.Fatal(err)
	}
}

func newApplication(output io.Writer) *application {
	events := logger.New(output, 128)
	appService := service.New(events)
	likes := queue.NewLikeQueue(128, func(job queue.LikeJob) error {
		_, err := appService.LikePost(job.PostID, job.Username)
		return err
	})

	mux := http.NewServeMux()
	mux.Handle("/debug/pprof/", http.DefaultServeMux)
	mux.Handle("/", handlers.New(appService, likes))

	return &application{
		Handler: mux,
		close: func() {
			likes.Close()
			events.Close()
		},
	}
}
