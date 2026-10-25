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

.PHONY: all build build-foreman build-gc build-foreman-image build-job-image test vet fmt check acceptance-census acceptance-census-refresh

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

# Acceptance matrix census: the design's TC numbers and the case names under
# tests/acceptance must name each other. The design document travels as a
# commit-pinned bundle under tests/testinfra/testdata, so no checkout is
# needed; FOREMAN_DESIGN_DIR overrides it with a local one at the same
# revision.
acceptance-census:
	go test ./tests/testinfra -run '^TestAcceptanceMatrixIsBidirectionallyClosed$$' -count=1 -v

# Refresh that bundle after the design moves, then pin the printed revision in
# acceptanceMatrixDesignAnchor. The bundle is written into this repo by
# absolute path (git -C would resolve a relative one inside the design
# checkout) and the superseded pin is dropped in the same step: one pin, one
# file.
acceptance-census-refresh:
	@test -n "$(FOREMAN_DESIGN_DIR)" || { echo "FOREMAN_DESIGN_DIR must name a tsic/foreman-design checkout" >&2; exit 2; }
	@short=$$(git -C "$(FOREMAN_DESIGN_DIR)" rev-parse --short=7 HEAD); \
	 data="$(CURDIR)/tests/testinfra/testdata"; \
	 out="$$data/foreman-design-$$short.bundle"; \
	 trap 'rm -f "$$out.tmp"' EXIT; \
	 git -C "$(FOREMAN_DESIGN_DIR)" bundle create "$$out.tmp" HEAD && \
	 mv "$$out.tmp" "$$out" && \
	 find "$$data" -name 'foreman-design-*.bundle' ! -name "foreman-design-$$short.bundle" -delete; \
	 remaining=$$(find "$$data" -name 'foreman-design-*.bundle' | wc -l | tr -d ' '); \
	 test "$$remaining" -eq 1 || { echo "expected exactly one pinned bundle in $$data, found $$remaining" >&2; exit 1; }; \
	 echo "bundle: $$out"
	@echo "pin acceptanceMatrixDesignAnchor to $$(git -C "$(FOREMAN_DESIGN_DIR)" rev-parse HEAD)"

fmt:
	gofmt -l .

check: fmt vet test build
