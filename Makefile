# Makefile for Spirit - MySQL table migration tool

# Build metadata injected via -ldflags
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT  ?= $(shell git rev-parse HEAD 2>/dev/null || echo "unknown")
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)

.PHONY: help setup-hooks lint lint-fix test build clean

help: ## Show this help message
	@echo "Spirit - MySQL Table Migration Tool"
	@echo ""
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

setup-hooks: ## Setup Git hooks for automatic linting
	@./scripts/setup-git-hooks.sh

lint: ## Run golangci-lint in Docker (read-only)
	@echo "Running golangci-lint..."
	@docker run --rm -v $$(pwd):/app -w /app golangci/golangci-lint:latest golangci-lint run --timeout=5m

lint-fix: ## Run golangci-lint with auto-fix enabled
	@echo "Running golangci-lint with auto-fix..."
	@docker run --rm -v $$(pwd):/app -w /app golangci/golangci-lint:latest golangci-lint run --fix --timeout=5m

test: ## Run all tests
	@echo "Running tests..."
	@go test ./... -v

build: ## Build the spirit binary (with version info)
	@echo "Building spirit $(VERSION)..."
	@go build -ldflags "$(LDFLAGS)" -o bin/spirit ./cmd/spirit

clean: ## Clean build artifacts
	@echo "Cleaning..."
	@rm -rf bin/
	@rm -f lint-error.logs

.DEFAULT_GOAL := help
