# Foreman v2 local build entry points.
#
# Local development only. Nothing here pushes, publishes or stamps a release;
# the release path is `.github/workflows/build-foreman.yml` (a `main` push
# publishes `foreman:latest`, a `v*.*.*` tag push exactly `foreman:v<X.Y.Z>`).

SHELL := /bin/sh
BIN_DIR ?= bin
VERSION ?= v2.0.0
REGISTRY ?= ghcr.io/tsic404
FOREMAN_IMAGE ?= $(REGISTRY)/foreman:$(VERSION)
CONTAINER_TOOL ?= $(shell command -v podman 2>/dev/null || command -v docker 2>/dev/null)
ARCH ?= $(shell go env GOARCH)
BASE_IMAGE ?= alpine:3.20

GOFLAGS ?= -trimpath
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: all build build-foreman build-foreman-image test vet fmt check

all: build

build: build-foreman

build-foreman:
	CGO_ENABLED=0 go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/foreman ./cmd/foreman

define build-linux-binary
CGO_ENABLED=0 GOOS=linux GOARCH=$(ARCH) go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(2) $(1)
endef

build-foreman-image:
	$(call build-linux-binary,./cmd/foreman,$(BIN_DIR)/foreman-linux-$(ARCH))
	$(CONTAINER_TOOL) build --platform linux/$(ARCH) -f images/foreman.Containerfile \
		--build-arg BASE_IMAGE=$(BASE_IMAGE) \
		-t $(FOREMAN_IMAGE) $(BIN_DIR)

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -l .

check: fmt vet test build