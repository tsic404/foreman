// Package injectproxy implements the fault-injection access path of the test
// multica cluster environment: a transparent HTTP/WS relay that sits between
// the Foreman instance under test and the test multica server.
//
// Contract: test-cases/_infra/test-multica-cluster.md §4. Three
// responsibilities, nothing else:
//
//   - transparency (§4.1): in mode "none" request, response and WS bytes are
//     relayed verbatim. The request head is never re-serialized, no header,
//     status or body byte is added, dropped or reordered, there is no retry
//     and no cache, and no multica daemon-API semantics are implemented. The
//     only actions are the configured faults (an HTTP status, a
//     connection-level close) and connection-level recording;
//   - the control plane (§4.2): GET /inject/state, POST /inject,
//     POST /inject/reset over terminal-5xx, upstream-5xx and ws-drop;
//   - the record face (§4.3): one JSONL row per request, response and WS byte
//     segment, with the raw bodies on disk and credentials reduced to their
//     prefix.
//
// The relay is connection-level: it parses exactly what it must observe (head
// bytes for the record face, paths for injection matching, body framing to
// know where a message ends) and forwards the bytes it read, never a
// re-rendering of them.
package injectproxy
