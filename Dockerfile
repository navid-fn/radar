# syntax=docker/dockerfile:1.4

# Single build stage for all binaries - shares module cache
FROM golang:1.24-alpine AS builder

WORKDIR /app

RUN apk add --no-cache gcc musl-dev

ENV GOPROXY=https://goproxy.io,direct

COPY go.mod go.sum ./

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# Build all binaries in parallel with shared cache
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/scraper ./cmd/scraper && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/ingester ./cmd/ingester && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/migrate ./cmd/migrate

# Scraper
FROM alpine:3.19 AS scraper
RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -g '' appuser
USER appuser
WORKDIR /app
COPY --from=builder /bin/scraper /app/scraper
ENTRYPOINT ["/app/scraper"]

# Ingester
FROM alpine:3.19 AS ingester
RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -g '' appuser
USER appuser
WORKDIR /app
COPY --from=builder /bin/ingester /app/ingester
ENTRYPOINT ["/app/ingester"]

# Migrate
FROM alpine:3.19 AS migrate
RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -g '' appuser
USER appuser
WORKDIR /app
COPY --from=builder /bin/migrate /app/migrate
COPY --from=builder /app/internal/migrations /app/internal/migrations
ENTRYPOINT ["/app/migrate -type=up"]

