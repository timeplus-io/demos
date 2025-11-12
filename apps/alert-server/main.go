package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

// AlertMessage represents an incoming alert
type AlertMessage struct {
	Title     string                 `json:"title"`
	Message   string                 `json:"message"`
	Severity  string                 `json:"severity"`
	Timestamp string                 `json:"timestamp"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// Broadcaster manages SSE connections
type Broadcaster struct {
	clients map[chan string]bool
	mu      sync.RWMutex
}

func NewBroadcaster() *Broadcaster {
	return &Broadcaster{
		clients: make(map[chan string]bool),
	}
}

func (b *Broadcaster) AddClient(ch chan string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.clients[ch] = true
}

func (b *Broadcaster) RemoveClient(ch chan string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.clients, ch)
	close(ch)
}

func (b *Broadcaster) Broadcast(message string) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch := range b.clients {
		select {
		case ch <- message:
		default:
			// Client is slow, skip
		}
	}
}

var broadcaster = NewBroadcaster()

func main() {
	http.HandleFunc("/alert", handleAlert)
	http.HandleFunc("/events", handleSSE)
	http.HandleFunc("/", handleIndex)

	port := "8080"
	log.Printf("Alert server starting on port %s", port)
	log.Printf("POST alerts to: http://localhost:%s/alert", port)
	log.Printf("View dashboard at: http://localhost:%s/", port)

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}

// handleAlert receives alert notifications via POST
func handleAlert(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var alert AlertMessage
	if err := json.Unmarshal(body, &alert); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Add timestamp if not provided
	if alert.Timestamp == "" {
		alert.Timestamp = time.Now().Format(time.RFC3339)
	}

	// Broadcast to all connected clients
	alertJSON, _ := json.Marshal(alert)
	broadcaster.Broadcast(string(alertJSON))

	log.Printf("Alert received: %s - %s", alert.Title, alert.Message)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, `{"status":"accepted","timestamp":"%s"}`, time.Now().Format(time.RFC3339))
}

// handleSSE streams alerts to clients via Server-Sent Events
func handleSSE(w http.ResponseWriter, r *http.Request) {
	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Create a channel for this client
	clientChan := make(chan string, 10)
	broadcaster.AddClient(clientChan)
	defer broadcaster.RemoveClient(clientChan)

	// Send initial connection message
	fmt.Fprintf(w, "data: {\"type\":\"connected\",\"message\":\"Connected to alert stream\"}\n\n")
	w.(http.Flusher).Flush()

	log.Printf("New client connected from %s", r.RemoteAddr)

	// Stream messages to client
	for {
		select {
		case msg := <-clientChan:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			w.(http.Flusher).Flush()
		case <-r.Context().Done():
			log.Printf("Client disconnected from %s", r.RemoteAddr)
			return
		}
	}
}

// handleIndex serves the HTML dashboard
func handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	http.ServeFile(w, r, "index.html")
}
