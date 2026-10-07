# Foreman build entry points (docs/04-architecture.md §部署/运行方式).

SHELL := /bin/sh
BIN_DIR ?= bin
VERSION ?= v0.1.0
REGISTRY ?= ghcr.io/tsic404
FOREMAN_IMAGE ?= $(REGISTRY)/foreman:$(VERSION)
JOB_IMAGE ?= $(REGISTRY)/foreman-job:$(VERSION)
CONTAINER_TOOL ?= $(shell command -v podman 2>/dev/null || command -v docker 2>/dev/null)

# Upstream release artifacts for the Job image (F1/AC-13): the CLI is taken from
# the release archive and both artifacts are checked against their SHA-256.
# `make resolve-upstream` prints the current URL + digest pairs.
MULTICA_CLI_URL ?=
MULTICA_CLI_SHA256 ?=
OMP_URL ?=
OMP_SHA256 ?=
BASE_IMAGE ?= alpine:3.20

# The delivery scripts read their inputs from the environment, so a
# `make publish-images VERSION=… REGISTRY=…` override reaches them.
export VERSION REGISTRY BASE_IMAGE CONTAINER_TOOL PUSH_LATEST
export MULTICA_CLI_URL MULTICA_CLI_SHA256 OMP_URL OMP_SHA256

GOFLAGS ?= -trimpath
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: all build build-foreman build-gc build-foreman-image build-job-image test vet fmt check \
	print-version resolve-upstream publish-images pin-job-image-digest check-deploy-digest smoke-job-image

all: build

build: build-foreman build-gc

build-foreman:
	CGO_ENABLED=0 go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/foreman ./cmd/foreman

build-gc:
	CGO_ENABLED=0 go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/foreman-gc ./cmd/foreman-gc

# Foreman's own image: the single-replica Deployment in deploy/30-foreman.yaml.
build-foreman-image: build-foreman
	$(CONTAINER_TOOL) build -f images/foreman.Containerfile \
		--build-arg BASE_IMAGE=$(BASE_IMAGE) \
		-t $(FOREMAN_IMAGE) $(BIN_DIR)

# Job image: upstream daemon CLI + omp, plus foreman-gc (the DaemonSet runs
# this same image). Publishes $(JOB_IMAGE); pin the pushed digest in deploy/.
build-job-image: build-gc
	$(CONTAINER_TOOL) build -f images/foreman-job.Containerfile \
		--build-arg BASE_IMAGE=$(BASE_IMAGE) \
		--build-arg MULTICA_CLI_URL=$(MULTICA_CLI_URL) \
		--build-arg MULTICA_CLI_SHA256=$(MULTICA_CLI_SHA256) \
		--build-arg OMP_URL=$(OMP_URL) \
		--build-arg OMP_SHA256=$(OMP_SHA256) \
		-t $(JOB_IMAGE) $(BIN_DIR)

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -l .

check: fmt vet test build

# Delivery chain (deploy/README.md §Images). CI is the release path; these are
# the same entry points it calls, runnable by hand against any registry.
print-version:
	@echo $(VERSION)

# Upstream Job-image artifacts for this platform: URL + the digest from the
# release's own checksum manifest (F1/AC-13).
resolve-upstream:
	scripts/resolve-upstream-artifacts.sh

# Build + push $(FOREMAN_IMAGE) and $(JOB_IMAGE), then print the <sha7>:<digest>
# mapping deploy/ pins. Needs MULTICA_CLI_URL/_SHA256 and OMP_URL/_SHA256.
publish-images:
	scripts/publish-images.sh

# Write the published Job digest into both deploy references.
pin-job-image-digest:
	scripts/deploy-digest.sh --pin $(DIGEST)

# Both deploy references exist, parse, and agree.
check-deploy-digest:
	scripts/deploy-digest.sh --check

# Pull the Job image and verify the digest (and the upstream CLI inside it).
smoke-job-image:
	scripts/smoke-job-image.sh $(IMAGE)
