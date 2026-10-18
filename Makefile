# Foreman local build entry points.
#
# Local development only: the release path is the two workflows in
# `.github/workflows/` (build-foreman.yml, build-foreman-job.yml), which build
# linux/amd64 + linux/arm64 themselves and push the multi-platform images to
# GHCR. Nothing here pushes, publishes or stamps a release.

SHELL := /bin/sh
BIN_DIR ?= bin
VERSION ?= v0.1.0
REGISTRY ?= ghcr.io/tsic404
FOREMAN_IMAGE ?= $(REGISTRY)/foreman:$(VERSION)
JOB_IMAGE ?= $(REGISTRY)/foreman-job:$(VERSION)
CONTAINER_TOOL ?= $(shell command -v podman 2>/dev/null || command -v docker 2>/dev/null)
# Platform of the local image builds (CI builds both); the Containerfile picks
# its per-arch artifact by buildx's TARGETARCH.
ARCH ?= $(shell go env GOARCH)
ARCH_UPPER := $(shell printf '%s' '$(ARCH)' | tr '[:lower:]' '[:upper:]')
BASE_IMAGE ?= alpine:3.20

# Upstream release artifact for the Job image (F1/AC-13): the CLI is taken from
# the release archive and checked against the SHA-256 from that release's
# checksum manifest. CI resolves both platforms itself; a local build passes the
# pair for $(ARCH). The agent backend `omp` is not an image input — the Job
# template installs it at start-up (ADR-033).
MULTICA_CLI_URL ?=
MULTICA_CLI_SHA256 ?=

GOFLAGS ?= -trimpath
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: all build build-foreman build-gc build-foreman-image build-job-image test vet fmt check

all: build

build: build-foreman build-gc

build-foreman:
	CGO_ENABLED=0 go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/foreman ./cmd/foreman

build-gc:
	CGO_ENABLED=0 go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/foreman-gc ./cmd/foreman-gc

# build-linux-binary <package> <output>: the static Linux/$(ARCH) binary the
# Containerfiles copy, named per platform so both can share a build context.
define build-linux-binary
CGO_ENABLED=0 GOOS=linux GOARCH=$(ARCH) go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(2) $(1)
endef

# Foreman's own image: the single-replica Deployment in deploy/30-foreman.yaml.
build-foreman-image:
	$(call build-linux-binary,./cmd/foreman,$(BIN_DIR)/foreman-linux-$(ARCH))
	$(CONTAINER_TOOL) build --platform linux/$(ARCH) -f images/foreman.Containerfile \
		--build-arg BASE_IMAGE=$(BASE_IMAGE) \
		-t $(FOREMAN_IMAGE) $(BIN_DIR)

# Job image: the upstream daemon CLI plus foreman-gc (the DaemonSet runs this
# same image) — no omp and no nix (ADR-033; the Job template's init step
# installs the agent binary). Tagged as $(JOB_IMAGE), referenced by movable tag;
# in CI its version tag is the carried daemon version.
build-job-image:
	$(call build-linux-binary,./cmd/foreman-gc,$(BIN_DIR)/foreman-gc-linux-$(ARCH))
	$(CONTAINER_TOOL) build --platform linux/$(ARCH) -f images/foreman-job.Containerfile \
		--build-arg BASE_IMAGE=$(BASE_IMAGE) \
		--build-arg MULTICA_CLI_URL_$(ARCH_UPPER)=$(MULTICA_CLI_URL) \
		--build-arg MULTICA_CLI_SHA256_$(ARCH_UPPER)=$(MULTICA_CLI_SHA256) \
		-t $(JOB_IMAGE) $(BIN_DIR)

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -l .

check: fmt vet test build
