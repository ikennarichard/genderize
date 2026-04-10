package api

import (
	"log"
	"net/http"
	"time"

	"github.com/ikennarichard/genderize/internal/handler"
)


func main() {
    mux := http.NewServeMux()
    mux.HandleFunc("GET /api/classify", handler.Classify)

    server := &http.Server{
        Addr:         ":8080",
        Handler:      mux,
        ReadTimeout:  10 * time.Second,
        WriteTimeout: 15 * time.Second,
        IdleTimeout:  60 * time.Second,
    }

    log.Println("Server starting on :8080")
    log.Fatal(server.ListenAndServe())
}