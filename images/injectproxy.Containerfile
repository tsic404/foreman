# Fault-injection access path (test infrastructure).
#
# Built from bin/ by `make build-injectproxy-image`; the binary is static
# (CGO_ENABLED=0). The record directory is an emptyDir: the record face is
# per-run evidence, read over the control plane (`GET /record`,
# `GET /record/blob`), so nothing needs to survive the pod.
ARG BASE_IMAGE=alpine:3.20
FROM ${BASE_IMAGE}

RUN apk add --no-cache ca-certificates \
 && addgroup -g 1000 injectproxy \
 && adduser -D -u 1000 -G injectproxy injectproxy \
 && mkdir -p /records \
 && chown 1000:1000 /records

COPY injectproxy /usr/local/bin/injectproxy

USER 1000:1000
ENTRYPOINT ["/usr/local/bin/injectproxy"]
