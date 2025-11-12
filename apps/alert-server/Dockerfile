# Build stage
FROM golang:1.22.5-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod ./

# Copy source code
COPY main.go ./

# Build the application
RUN go build -o alert-server main.go

# Runtime stage
FROM alpine:latest

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/alert-server .

# Copy the HTML file
COPY index.html .

# Expose port
EXPOSE 8080

# Run the application
CMD ["./alert-server"]