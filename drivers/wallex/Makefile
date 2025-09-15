# Makefile for Radar Go Application

.PHONY: build run clean docker-build docker-run test deps help

# Variables
APP_NAME := radar-go
DOCKER_IMAGE := radar-go:latest
KAFKA_BROKER := localhost:9092

# Default target
help: ## Show this help message
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development
deps: ## Download and install dependencies
	go mod tidy
	go mod download

build: deps ## Build the application
	go build -o $(APP_NAME) main.go

run: build ## Run the application locally
	KAFKA_BROKER=$(KAFKA_BROKER) ./$(APP_NAME)

test: ## Run tests
	go test -v ./...

clean: ## Clean build artifacts
	rm -f $(APP_NAME)
	go clean

##@ Docker
docker-build: ## Build Docker image
	docker build -f Dockerfile.go -t $(DOCKER_IMAGE) .

docker-run: docker-build ## Run application in Docker container
	docker run --rm --name $(APP_NAME) \
		-e KAFKA_BROKER=$(KAFKA_BROKER) \
		$(DOCKER_IMAGE)

docker-compose-up: ## Start all services with docker-compose
	docker-compose up --build -d

docker-compose-logs: ## View logs from docker-compose services
	docker-compose logs -f radar-go

docker-compose-down: ## Stop all docker-compose services
	docker-compose down

##@ Utilities
fmt: ## Format Go code
	go fmt ./...

lint: ## Run linter (requires golangci-lint)
	golangci-lint run

vet: ## Run go vet
	go vet ./...

mod-update: ## Update Go modules
	go get -u ./...
	go mod tidy

##@ Production
build-linux: deps ## Build for Linux (production)
	GOOS=linux GOARCH=amd64 CGO_ENABLED=1 go build -a -installsuffix cgo -o $(APP_NAME)-linux main.go

release: clean build-linux ## Create release build
	tar -czf $(APP_NAME)-linux.tar.gz $(APP_NAME)-linux README-GO.md
