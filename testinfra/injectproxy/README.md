# Fault-injection access path (`injectproxy`)

Test infrastructure of the test multica cluster environment: a transparent
HTTP/WS relay that sits between the Foreman instance under test and the test
multica server. It is the component behind the stable access point
(`http://<test-multica-access>`) and the control/record face
(`http://<test-multica-control>`) of
`test-cases/_infra/test-multica-cluster.md` §2/§4 — the contract this
implementation answers to.

It is **not** part of the Foreman deployment: `deploy/` stays the product's
manifests, this component is deployed by the test-environment wiring.

## What it does

- **Transparent relay (§4.1).** In mode `none` request, response and WS bytes
  are forwarded verbatim. The request head is read off the wire and written to
  the upstream unchanged — it is never re-serialized, so header casing, order,
  duplicates, the query string and the body are preserved byte for byte. No
  retry, no cache, no protocol semantics: the relay understands exactly what it
  must observe (paths for injection matching, body framing to know where a
  message ends, head bytes for the record face).
- **Fault injection (§4.2).** `terminal-5xx` and `upstream-5xx` answer matching
  requests locally with the configured status (they never reach the server);
  `ws-drop` closes the established WS connections and refuses new upgrades for
  its window while HTTP keeps flowing.
- **Record face (§4.3).** One JSONL row per request, per recorded response
  body and per WS byte segment, plus the raw bodies on disk. Credentials are
  reduced to `mdt_`/`mul_`/`mat_`/`fmj_`/`other` — the token itself never
  reaches a record file or a log line.

## Configuration

| Env | Default | Meaning |
|---|---|---|
| `INJECT_UPSTREAM_URL` | — (required) | Test multica server (`http://` or `https://`); the data plane relays to it |
| `INJECT_DATA_ADDR` | `:8080` | Transparent data plane: the stable access point |
| `INJECT_CONTROL_ADDR` | `:8081` | Injection + record control plane (separate port/Service) |
| `INJECT_RECORD_DIR` | `./records` | `records.jsonl`, `bodies/`, `ws/` |
| `INJECT_RECORD_RESPONSE_BODIES` | `^/api/daemon/tasks/claim$` | Comma-separated path regexes whose **response** bodies are written to disk; `^$` records status codes only |
| `INJECT_LOG_LEVEL` | `info` | `debug`\|`info`\|`warn`\|`error` |

## Control plane

| Operation | Request | Semantics |
|---|---|---|
| Read state | `GET /inject/state` | `{"mode","path_regex","status","until"}`; `until` is RFC3339 or `null` |
| Set injection | `POST /inject` with `{"mode","path_regex","status","duration_s"}` | Takes effect immediately, replaces the active mode, resets itself when `duration_s` elapses (`0`/absent = until reset) |
| Reset | `POST /inject/reset` | Equivalent to `mode=none` |
| Read records | `GET /record?since=<RFC3339>&limit=N` | JSON array in append order; `since` is exclusive, `limit` `0`/absent = all |
| Read a body | `GET /record/blob?path=bodies/0001.bin` | Raw bytes of a row's `body_path`/`bytes_path`; only inside the record directory |

`path_regex` matches the request path **without** query; empty matches every
path. Modes and statuses: `terminal-5xx` (default `503`), `upstream-5xx`
(default `500`), `ws-drop` (status is meaningless). Unknown modes, bad regexes,
statuses outside 100..599, a `duration_s` outside `0..1000000` and unknown JSON
keys are `400`.

```bash
CONTROL=http://<test-multica-control>

curl -s $CONTROL/inject/state                       # {"mode":"none",...}
curl -s -XPOST $CONTROL/inject -d '{"mode":"terminal-5xx","path_regex":"^/api/daemon/tasks/[^/]+/(complete|fail|cancel-ack)$","status":503,"duration_s":300}'
curl -s -XPOST $CONTROL/inject -d '{"mode":"ws-drop","duration_s":120}'
curl -s -XPOST $CONTROL/inject/reset

curl -s "$CONTROL/record?since=2026-10-13T00:00:00Z&limit=100" | jq '[.[] | select(.path=="/api/daemon/tasks/claim")]'
curl -s "$CONTROL/record/blob?path=bodies/0001.bin" | jq '.tasks[0]'
```

## Record face

```
<INJECT_RECORD_DIR>/
  records.jsonl     # one row per request / response body / WS byte segment
  bodies/0001.bin   # request bodies (every request) + recorded response bodies
  ws/0001.bin       # raw WS bytes, one file per read segment
```

```json
{"ts":"2026-10-13T01:09:34.665119Z","direction":"F→S","kind":"http","method":"POST","path":"/api/daemon/tasks/<tid>/complete","authorization_prefix":"fmj_","headers":{"X-Client-Capabilities":"..."},"body_sha256":"...","body_path":"bodies/0007.bin","status":200,"injected":false}
```

- `direction`: `F→S` = Foreman's own credential (`mdt_`/`mul_`), `A→S` = every
  other credential face (`mat_`, probe credentials, no credential), `S→F` =
  the server's responses and its WS downlink bytes.
- `kind`: `http` or `ws-chunk`. A `ws-chunk` row carries `bytes_path` (raw
  bytes, no frame parsing) instead of a body; `method`/`status` are absent.
- `headers` is an allowlist (`X-Client-Capabilities`) — a free-form header map
  is where credentials would leak.
- `body_sha256`/`body_path`: the **decoded** body (a chunked body is
  de-chunked for the record; the wire keeps its framing). Every request body is
  written; an empty body hashes to `sha256("")` and leaves no file — a bodyless
  request leaves nothing in `bodies/` at all.
- Every request the access path saw has a row, including one whose exchange died
  before the upstream answered (dial failure, an upstream that closed early):
  that row carries no `status` rather than an invented one. A row is also
  bounded — an oversized header value or path is truncated with a trailing `…`,
  so one request cannot produce a line the read face could not scan back.
- Response bodies: only paths matching `INJECT_RECORD_RESPONSE_BODIES` are
  written, as their own `S→F` row that keeps the request's `path` and omits
  `method`. Every other response contributes only its `status` to the request
  row.
- `injected: true` marks a request the injector answered itself (a 5xx mode) or
  refused (`ws-drop`) — it never reached the server. An injected request row
  carries the same `body_sha256` as the identical request forwarded later.

`ts` is the event time (request receipt, response head, WS segment read); rows
are appended when the event completes, so a record read after a probe returns
sees its rows.

## Build and run

```bash
make build-injectproxy         # bin/injectproxy
make build-injectproxy-image   # ghcr.io/tsic404/foreman-injectproxy:<version>

INJECT_UPSTREAM_URL=http://127.0.0.1:9000 \
INJECT_DATA_ADDR=127.0.0.1:8080 \
INJECT_CONTROL_ADDR=127.0.0.1:8081 \
INJECT_RECORD_DIR=/tmp/records \
  ./bin/injectproxy
```

The image is published by `.github/workflows/injectproxy-image.yml` on pushes
touching this component; a locally built image can be imported into the test
cluster instead (`k3d image import`, `docker save | ctr -n k8s.io images
import`).

## Deployment

`testinfra/injectproxy/deploy.yaml` is the starting point: a Deployment with
the two container ports and the record `emptyDir`, plus one Service per face
(`injectproxy-data`, `injectproxy-control`). Apply it into the test cluster's
namespace and adapt the names, the upstream address and the image reference to
the environment. The control plane carries no external exposure — the test
network reaches it however the environment does (port-forward, an internal
load balancer, a NetworkPolicy-restricted ingress); the data plane is what
`MULTICA_SERVER_URL` points at.

Two things the manifest cannot carry for you:

- **Pull secret.** `ghcr.io/tsic404/foreman-injectproxy` is private like the
  product images, so the namespace needs a `registry-tsic` docker-registry
  secret (`deploy/README.md` §Images has the `kubectl create secret
  docker-registry` invocation) and the Pod references it through
  `imagePullSecrets`.
- **`strategy: Recreate`.** Injection state and `records.jsonl` live in the
  process and in a per-Pod `emptyDir`. A rolling update would leave two Ready
  Pods behind the two Services for a moment, and an injection set on one Pod
  would not apply to traffic served by the other.

## Boundaries of the implementation

- Plain HTTP/1.1 and WS upgrades (the access point is `http://`). `https`
  upstreams are relayed with the system trust store; there is no TLS
  termination and no HTTP/2.
- One upstream connection per client connection, no pipelining: the relay
  answers requests in the order they arrive.
- `Expect: 100-continue` is relayed as the server's own interim response. When
  the fault is injected on such a request, the injected status ends the
  connection: the unread body could not be told apart from the next request
  head, and §4.1 forbids guessing at it. The same rule applies whenever a
  response arrives before the request body was consumed.
- No read deadlines: slow or long-lived traffic (the WS channel lives for
  hours) is never cut. Only dialing the upstream is bounded.
- The injector is a single point of failure by design (§8 of the contract): it
  carries no functional correctness, and its own failure looks like a full
  test-environment outage. Diagnose it through the environment self-checks
  (§2/§6), not through the use cases.
