# Stackia Search — build & run helpers.

BINARY   := stackia
PKG      := ./cmd/stackia
GOFLAGS  := -trimpath
LDFLAGS  := -s -w

# Raspberry Pi (64-bit, Pi 3/4/5) cross-compile target.
ARM64_ENV := GOOS=linux GOARCH=arm64
# Raspberry Pi (32-bit, older Pi / Pi Zero 2) target.
ARMV7_ENV := GOOS=linux GOARCH=arm GOARM=7

.PHONY: all build run tidy test vet fmt clean pi pi32

all: build

## build: compile the host binary
build:
	go build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BINARY) $(PKG)

## run: build and run with the default config.yaml
run: build
	./$(BINARY) -config config.yaml

## pi: cross-compile a stripped 64-bit Raspberry Pi binary
pi:
	$(ARM64_ENV) go build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BINARY)-arm64 $(PKG)
	@echo "built $(BINARY)-arm64 — scp it to your Pi alongside config.yaml"

## pi32: cross-compile a stripped 32-bit (ARMv7) Raspberry Pi binary
pi32:
	$(ARMV7_ENV) go build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BINARY)-armv7 $(PKG)

## tidy: sync go.mod/go.sum
tidy:
	go mod tidy

## vet: static analysis
vet:
	go vet ./...

## fmt: format the tree
fmt:
	gofmt -s -w .

## test: run unit tests
test:
	go test ./...

## clean: remove build output and runtime data
clean:
	rm -f $(BINARY) $(BINARY)-arm64 $(BINARY)-armv7
	rm -rf data
