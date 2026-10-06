# Foreman build entry points (docs/04-architecture.md §部署/运行方式).

SHELL := /bin/sh
BIN_DIR ?= bin
VERSION ?= v0.1.0
REGISTRY ?= registry.tsic.top/multica
FOREMAN_IMAGE ?= $(REGISTRY)/foreman:$(VERSION)
JOB_IMAGE ?= $(REGISTRY)/foreman-job:$(VERSION)
CONTAINER_TOOL ?= $(shell command -v podman 2>/dev/null || command -v docker 2>/dev/null)

# Upstream release artifacts for the Job image (F1/AC-13): the CLI binary is
# used verbatim and both artifacts are checked against their SHA-256.
MULTICA_CLI_URL ?=
MULTICA_CLI_SHA256 ?=
OMP_URL ?=
OMP_SHA256 ?=
BASE_IMAGE ?= alpine:3.20

GOFLAGS ?= -trimpath
LDFLAGS := -s -w -X main.version=$(VERSION)

# Publishing / delivery chain (deploy/README.md §Images). These are the same
# entry points CI uses — never a hand-rolled push:
#   make publish-images   build + push both images, write dist/release-images.env
#   make pin-digest       write the pushed Job digest into both deploy files
#   make verify-delivery  registry API/TLS + pinned digests (CLUSTER=1: pods)
#   make qa-registry-trust  install the registry CA into a k3d QA node
export VERSION REGISTRY BASE_IMAGE CONTAINER_TOOL

.PHONY: all build build-foreman build-gc build-foreman-image build-job-image test vet fmt check \
	publish-images pin-digest verify-delivery qa-registry-trust

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

# --- delivery chain ---------------------------------------------------------

# Build and push both images, emit the <sha7>:<job-digest> mapping. Requires
# MULTICA_CLI_URL/_SHA256 and OMP_URL/_SHA256 (upstream release artifacts).
publish-images:
	./scripts/publish-images.sh

# Pin the pushed Job image digest into both deploy files:
#   make pin-digest DIGEST=sha256:…   |   make pin-digest FROM=dist/release-images.env
pin-digest:
	./scripts/pin-job-image-digest.sh $(if $(FROM),--from-file $(FROM),--digest $(DIGEST))

# Check registry API/TLS, published tags and the deploy-pinned digest.
# CLUSTER=1 also checks the deployed pods for ErrImagePull/ImagePullBackOff.
verify-delivery:
	./scripts/verify-delivery.sh $(if $(CA_FILE),--ca $(CA_FILE),) $(if $(filter 1,$(CLUSTER)),--cluster,)

# Install the registry CA into a k3d QA node (NODE=k3d-test-server-0 CA=… HOST_IP=…).
qa-registry-trust:
	./scripts/qa-registry-trust.sh --node $(NODE) --ca $(CA) $(if $(HOST_IP),--host-ip $(HOST_IP),)
