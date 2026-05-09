.PHONY: all clean test lint dev-deps audit-cgi

BUILD_DIR=bin

all: bin/pgjsontool bin/crossrefimport bin/crossreffilter bin/crossrefcachebuild bin/crossrefclassify bin/crossrefstagefromjsonl
	@echo "Build complete"

bin/pgjsontool: cmd/pgjsontool/main.go
	@mkdir -p bin
	go build -o bin/pgjsontool ./cmd/pgjsontool

bin/crossrefimport: cmd/crossrefimport/main.go
	@mkdir -p bin
	go build -o bin/crossrefimport ./cmd/crossrefimport

bin/crossreffilter: cmd/crossreffilter/main.go
	@mkdir -p bin
	go build -o bin/crossreffilter ./cmd/crossreffilter

bin/crossrefcachebuild: cmd/crossrefcachebuild/main.go
	@mkdir -p bin
	go build -o bin/crossrefcachebuild ./cmd/crossrefcachebuild

bin/crossrefclassify: cmd/crossrefclassify/main.go
	@mkdir -p bin
	go build -o bin/crossrefclassify ./cmd/crossrefclassify

bin/crossrefstagefromjsonl: cmd/crossrefstagefromjsonl/main.go
	@mkdir -p bin
	go build -o bin/crossrefstagefromjsonl ./cmd/crossrefstagefromjsonl

clean:
	@echo "Cleaning..."
	@rm -rf bin
	@$(MAKE) -C audit_cgi clean >/dev/null 2>&1 || true

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

audit-cgi:
	@$(MAKE) -C audit_cgi
