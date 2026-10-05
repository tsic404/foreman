// Package proxy implements the bidirectional protocol translation: a fake
// client towards the Multica server (register/heartbeat/claim/lease/
// forward/WS subscribe) and a fake server towards the Job daemons
// (auth/delivery/report forwarding/agent passthrough/WS endpoint).
// Design: docs/05-modules/proxy.md, contracts §1.1/§1.2/§4, ADR-010.
package proxy
