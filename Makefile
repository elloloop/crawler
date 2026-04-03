.PHONY: proto build run test clean docker-build docker-up docker-down lint

BINARY := crawler
GO := go
BUF := buf

# Generate protobuf code
proto:
	$(BUF) lint
	$(BUF) generate

# Build the binary
build:
	$(GO) build -o bin/$(BINARY) ./cmd/server

# Run locally
run: build
	./bin/$(BINARY)

# Run tests
test:
	$(GO) test ./... -v -race

# Lint protos
lint:
	$(BUF) lint

# Clean build artifacts
clean:
	rm -rf bin/

# Docker
docker-build:
	docker build -t $(BINARY) .

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f crawler
