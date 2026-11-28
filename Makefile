# Foreman local build entry points.
#
# Local development only: the release path is the workflow in
# `.github/workflows/` (build-foreman.yml), which builds linux/amd64 +
# linux/arm64 itself and pushes the multi-platform image to GHCR. Nothing here
# pushes, publishes or stamps a release.

SHELL := /bin/sh
BIN_DIR ?= bin
VERSION ?= v0.1.0
REGISTRY ?= ghcr.io/tsic404
FOREMAN_IMAGE ?= $(REGISTRY)/foreman:$(VERSION)
CONTAINER_TOOL ?= $(shell command -v podman 2>/dev/null || command -v docker 2>/dev/null)
# Platform of the local image builds (CI builds both); the Containerfile picks
# its per-arch artifact by buildx's TARGETARCH.
ARCH ?= $(shell go env GOARCH)
BASE_IMAGE ?= alpine:3.20

GOFLAGS ?= -trimpath
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: all build build-foreman build-gc build-job build-job-adapters build-foreman-image test vet fmt check acceptance-census acceptance-census-refresh

all: build

build: build-foreman build-gc build-job build-job-adapters

build-foreman:
	CGO_ENABLED=0 go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/foreman ./cmd/foreman

build-gc:
	CGO_ENABLED=0 go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/foreman-gc ./cmd/foreman-gc

build-job:
	CGO_ENABLED=0 go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/foreman-job ./cmd/foreman-job

# The reference Job adapters (V2-C29). They ship inside the runtime image
# beside foreman-job and are copied into the Job's runtime volume by the
# job-runtime init container; a deployment that supplies its own adapters
# replaces the runtime image and leaves these alone.
JOB_ADAPTERS := foreman-job-cli rsbx-remote foreman-job-register foreman-job-runner foreman-job-slots

build-job-adapters:
	@for cmd in $(JOB_ADAPTERS); do \
		CGO_ENABLED=0 go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/$$cmd ./cmd/$$cmd || exit 1; \
	done

# build-linux-binary <package> <output>: the static Linux/$(ARCH) binary the
# Containerfiles copy, named per platform so both can share a build context.
define build-linux-binary
CGO_ENABLED=0 GOOS=linux GOARCH=$(ARCH) go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(2) $(1)
endef

# Foreman's own image: the Deployment, the Job-side supervisor and the gc
# DaemonSet all run from it. The Job carrier is the external rsbx image.
build-foreman-image:
	$(call build-linux-binary,./cmd/foreman,$(BIN_DIR)/foreman-linux-$(ARCH))
	$(call build-linux-binary,./cmd/foreman-job,$(BIN_DIR)/foreman-job-linux-$(ARCH))
	$(call build-linux-binary,./cmd/foreman-gc,$(BIN_DIR)/foreman-gc-linux-$(ARCH))
	@for cmd in $(JOB_ADAPTERS); do \
		$(call build-linux-binary,./cmd/$$cmd,$(BIN_DIR)/$$cmd-linux-$(ARCH)) || exit 1; \
	done
	$(CONTAINER_TOOL) build --platform linux/$(ARCH) -f images/foreman.Containerfile \
		--build-arg BASE_IMAGE=$(BASE_IMAGE) \
		-t $(FOREMAN_IMAGE) $(BIN_DIR)

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
