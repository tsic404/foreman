#!/usr/bin/env bash
# Make a k3d QA cluster actually trust the private registry: per-registry CA in
# the node's containerd config plus the CA in the node's system trust store —
# never `--tls-skip-verify` (deploy/README.md §QA cluster registry access).
#
#   scripts/qa-registry-trust.sh --node k3d-test-server-0 --ca <pem> --host-ip <ip>
#
# --no-restart leaves the trust inert: containerd reads registries.yaml only at
# start. Run it where the k3d node container is visible (the docker host).
set -euo pipefail

NODE=""
CA=""
REGISTRY_HOST=${REGISTRY_HOST:-registry.tsic.top}
HOST_IP=""
RESTART=1

die() { printf 'qa-registry-trust: %s\n' "$*" >&2; exit 1; }
note() { printf 'qa-registry-trust: %s\n' "$*"; }

while [ $# -gt 0 ]; do
  case "$1" in
    --node) NODE=${2:-}; shift 2 ;;
    --ca) CA=${2:-}; shift 2 ;;
    --registry-host) REGISTRY_HOST=${2:-}; shift 2 ;;
    --host-ip) HOST_IP=${2:-}; shift 2 ;;
    --no-restart) RESTART=0; shift ;;
    -h|--help) sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown argument $1" ;;
  esac
done

[ -n "$NODE" ] || die "--node is required"
[ -n "$CA" ] || die "--ca is required"
[ -f "$CA" ] || die "CA file $CA not found"
docker inspect "$NODE" >/dev/null 2>&1 || die "container $NODE not found (run this on the docker host of the k3d cluster)"

# 1. CA into the node: per-registry trust for containerd + system trust store.
docker cp "$CA" "$NODE:/etc/rancher/k3s/tsic-registry-ca.crt"
docker exec "$NODE" sh -c 'set -e; bundle=/etc/ssl/certs/ca-certificates.crt; \
  if [ -f "$bundle" ] && ! grep -qF "tsic-registry-ca" "$bundle"; then cat /etc/rancher/k3s/tsic-registry-ca.crt >> "$bundle"; fi'

# 2. registries.yaml: mirror the registry hostname to the reachable endpoint and
#    pin the CA for both names. k3s renders this into containerd certs.d at start.
docker exec -i "$NODE" sh -c "cat > /etc/rancher/k3s/registries.yaml" <<EOF
# Written by scripts/qa-registry-trust.sh — QA wiring for the private registry.
mirrors:
  "$REGISTRY_HOST":
    endpoint:
      - "https://${HOST_IP:-$REGISTRY_HOST}"
configs:
  "$REGISTRY_HOST":
    tls:
      ca_file: /etc/rancher/k3s/tsic-registry-ca.crt
EOF
if [ -n "$HOST_IP" ]; then
  docker exec -i "$NODE" sh -c "cat >> /etc/rancher/k3s/registries.yaml" <<EOF
  "$HOST_IP":
    tls:
      ca_file: /etc/rancher/k3s/tsic-registry-ca.crt
EOF
fi

# 3. containerd reads registries.yaml only at start.
if [ "$RESTART" = "1" ]; then
  note "restarting $NODE to load the registry trust"
  docker restart "$NODE" >/dev/null
  for _ in $(seq 1 60); do
    docker exec "$NODE" sh -c 'test -S /run/k3s/containerd/containerd.sock' 2>/dev/null && break
    sleep 2
  done
fi

# 4. Node-side name resolution for tools that do not go through containerd's
#    mirror (curl/wget inside the node, crictl by name).
if [ -n "$HOST_IP" ]; then
  docker exec "$NODE" sh -c "grep -q '[[:space:]]$REGISTRY_HOST\$' /etc/hosts || echo '$HOST_IP $REGISTRY_HOST' >> /etc/hosts"
fi

note "trust installed for $REGISTRY_HOST (endpoint ${HOST_IP:-$REGISTRY_HOST})"
note "verify with: docker exec $NODE crictl pull $REGISTRY_HOST/$(printf '%s' "${PROBE_IMAGE:-multica/foreman-job}:${PROBE_TAG:-v0.1.0}")"
