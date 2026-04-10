package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

type GenderizeResponse struct {
    Name        string  `json:"name"`
    Gender      *string `json:"gender"`
    Probability float64 `json:"probability"`
    Count       int     `json:"count"`
}

type SuccessResponse struct {
    Status string `json:"status"`
    Data   Data   `json:"data"`
}

type Data struct {
    Name        string    `json:"name"`
    Gender      string    `json:"gender"`
    Probability float64   `json:"probability"`
    SampleSize  int       `json:"sample_size"`
    IsConfident bool      `json:"is_confident"`
    ProcessedAt string    `json:"processed_at"`
}

type ErrorResponse struct {
    Status  string `json:"status"`
    Message string `json:"message"`
}

var client = &http.Client{
    Timeout: 8 * time.Second,
}

func Classify(w http.ResponseWriter, r *http.Request) {
	// start := time.Now() 
    name := r.URL.Query().Get("name")

    if err := validateName(name); err != nil {
        respondWithJSON(w, http.StatusBadRequest, ErrorResponse{
            Status:  "error",
            Message: err.Error(),
        })
        return
    }

    apiResp, err := callGenderizeAPI(name)
    if err != nil {
        respondWithJSON(w, http.StatusBadGateway, ErrorResponse{
            Status:  "error",
            Message: err.Error(),
        })
        return
    }

    if apiResp.Gender == nil || apiResp.Count == 0 {
        respondWithJSON(w, http.StatusUnprocessableEntity, ErrorResponse{
            Status:  "error",
            Message: "No prediction available for the provided name",
        })
        return
    }

    data := buildProcessedData(apiResp)

    respondWithJSON(w, http.StatusOK, SuccessResponse{
        Status: "success",
        Data:   data,
    })
		//   internalTime := time.Since(start).Milliseconds()
    // log.Printf("Internal processing time for name '%s': %d ms", name, internalTime)
}

func respondWithJSON(w http.ResponseWriter, status int, payload any) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(status)
    if err := json.NewEncoder(w).Encode(payload); err != nil {
        http.Error(w, `{"status":"error","message":"internal server error"}`, http.StatusInternalServerError)
    }
}

func validateName(name string) error {
    if name == "" {
        return fmt.Errorf("name parameter is required")
    }
    if len(name) > 100 {
        return fmt.Errorf("name is too long (max 100 characters)")
    }
    return nil
}

func callGenderizeAPI(name string) (*GenderizeResponse, error) {
    resp, err := client.Get("https://api.genderize.io/?name=" + name)
    if err != nil {
        return nil, fmt.Errorf("failed to connect to genderize api")
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return nil, fmt.Errorf("upstream API error")
    }

    var apiResp GenderizeResponse
    if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
        return nil, fmt.Errorf("failed to parse upstream response")
    }

    return &apiResp, nil
}

func buildProcessedData(apiResp *GenderizeResponse) Data {
    isConfident := *apiResp.Gender != "" &&
        apiResp.Probability >= 0.7 &&
        apiResp.Count >= 100

    return Data{
        Name:        apiResp.Name,
        Gender:      *apiResp.Gender,
        Probability: apiResp.Probability,
        SampleSize:  apiResp.Count,
        IsConfident: isConfident,
        ProcessedAt: time.Now().UTC().Format(time.RFC3339),
    }
}

func main() {
    mux := http.NewServeMux()
    mux.HandleFunc("GET /api/classify", Classify)

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