# Alert Server Demo

A simple, stateless alert notification server with real-time web UI built with Go and Server-Sent Events (SSE).

## Quick Start

### 1. Run the server

```bash
go run main.go
```

The server will start on port 8080:
- Web Dashboard: http://localhost:8080/
- Alert Endpoint: http://localhost:8080/alert
- SSE Stream: http://localhost:8080/events

### 2. Open the dashboard

Open your browser and navigate to:
```
http://localhost:8080/
```

### 3. Send alerts

Use curl, your application, or any HTTP client to send alerts:

```bash
curl -X POST http://localhost:8080/alert \
  -H "Content-Type: application/json" \
  -d '{
    "title": "High CPU Usage",
    "message": "Server CPU usage exceeded 90%",
    "severity": "critical"
  }'
```

## API Reference

### POST /alert

Send an alert notification to be displayed on all connected dashboards.

**Request Body:**

```json
{
  "title": "Alert Title",
  "message": "Detailed alert message",
  "severity": "critical",
  "timestamp": "2025-11-12T10:30:00Z",
  "metadata": {
    "host": "server-01",
    "region": "us-west-2",
    "custom_field": "custom_value"
  }
}
```

**Fields:**
- `title` (string, required): Alert title
- `message` (string, required): Alert description
- `severity` (string, optional): One of `critical`, `warning`, `info`, `success`. Default: `info`
- `timestamp` (string, optional): ISO 8601 timestamp. Auto-generated if not provided
- `metadata` (object, optional): Any additional key-value pairs to display

**Response:**

```json
{
  "status": "accepted",
  "timestamp": "2025-11-12T10:30:00Z"
}
```

## Example Alerts

### Critical Alert
```bash
curl -X POST http://localhost:8080/alert \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Database Connection Failed",
    "message": "Unable to connect to primary database. Failover initiated.",
    "severity": "critical",
    "metadata": {
      "database": "postgres-primary",
      "error": "connection timeout"
    }
  }'
```

### Warning Alert
```bash
curl -X POST http://localhost:8080/alert \
  -H "Content-Type: application/json" \
  -d '{
    "title": "High Memory Usage",
    "message": "Memory usage at 85%. Consider scaling up.",
    "severity": "warning",
    "metadata": {
      "host": "api-server-03",
      "memory_used": "85%"
    }
  }'
```

### Info Alert
```bash
curl -X POST http://localhost:8080/alert \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Deployment Started",
    "message": "Application version 2.1.0 deployment initiated",
    "severity": "info",
    "metadata": {
      "version": "2.1.0",
      "environment": "production"
    }
  }'
```

### Success Alert
```bash
curl -X POST http://localhost:8080/alert \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Deployment Complete",
    "message": "Application successfully deployed and health checks passed",
    "severity": "success",
    "metadata": {
      "version": "2.1.0",
      "duration": "3m 24s"
    }
  }'
```

## Building for Production

### Build binary
```bash
go build -o alert-server
```

### Run binary
```bash
./alert-server
```

### Build for different platforms
```bash
# Linux
GOOS=linux GOARCH=amd64 go build -o alert-server-linux

# macOS
GOOS=darwin GOARCH=amd64 go build -o alert-server-macos

# Windows
GOOS=windows GOARCH=amd64 go build -o alert-server.exe
```

## Configuration

By default, the server runs on port 8080. To change the port, modify the `port` variable in `main.go`:

```go
port := "8080"  // Change to your desired port
```