# Use multi-stage build
FROM golang:1.22-alpine AS builder

# Set working directory
WORKDIR /app

# Install necessary build tools
RUN apk add --no-cache git gcc musl-dev

# Copy go.mod and go.sum
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build application
RUN CGO_ENABLED=1 GOOS=linux go build -o utxo-indexer main.go

# Use lightweight alpine as runtime environment
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Set timezone
ENV TZ=Asia/Shanghai

# Create data directory
RUN mkdir -p /app/data

# Copy compiled application from builder stage
COPY --from=builder /app/utxo-indexer /app/
COPY --from=builder /app/config.yaml /app/

# Set working directory
WORKDIR /app

# Expose API port
EXPOSE 8080

# Run application
CMD ["./utxo-indexer"] 