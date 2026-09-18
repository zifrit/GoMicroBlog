package main

import (
	"log"
	"net/http"

	"MicroBlog/internal/handlers"
	"MicroBlog/internal/service"
)

func main() {
	appService := service.New()
	httpHandler := handlers.New(appService)

	log.Println("microblog server is running on http://localhost:8080")
	if err := http.ListenAndServe(":8080", httpHandler); err != nil {
		log.Fatal(err)
	}
}
