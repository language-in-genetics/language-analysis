.PHONY: all build clean test lint

# Binary name
BINARY_NAME=jsonreader
# Build directory
BUILD_DIR=bin

all: clean build

build:
	@echo "Building..."
	@mkdir -p $(BUILD_DIR)
	@go build -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/jsonreader

clean:
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR)

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
	@./$(BUILD_DIR)/$(BINARY_NAME)

# Run with specific directory
run-dir: build
	@./$(BUILD_DIR)/$(BINARY_NAME) -dir $(dir)
