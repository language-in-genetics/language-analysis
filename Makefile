.PHONY: all build clean test lint

# Binary name
BINARY_NAME=jsonreader
# Build directory
BUILD_DIR=bin

all: bin/jsonreader bin/pgjsontool
	echo Done

bin/jsonreader: cmd/jsonreader/main.go
	@mkdir -p bin
	go build -o bin/jsonreader ./cmd/jsonreader

bin/pgjsontool: cmd/pgjsontool/main.go
	mkdir -p bin
	go build -o bin/pgjsontool ./cmd/pgjsontool


clean:
	@echo "Cleaning..."
	@rm -rf bin

test:
	@echo "Running tests..."
	@go test -v ./...

lint:
	@echo "Running linter..."
	@if command -v golangci-lint > /dev/null; then \
		golangci-lint run; \
	else \
		echo "golangci-lint is not installed"; \
		exit 1; \
	fi

# Install development dependencies
dev-deps:
	@echo "Installing development dependencies..."
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Run the program with default settings
run: build
	@./bin/$(BINARY_NAME)

# Run with specific directory
run-dir: build
	@./bin/$(BINARY_NAME) -dir $(dir)
