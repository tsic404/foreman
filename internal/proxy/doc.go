// Package proxy implements the bidirectional protocol translation: a fake
// client towards the Multica server (register/heartbeat/claim/lease/
// forward/WS subscribe) and a fake server towards the Job daemons
// (auth/delivery/report forwarding/agent passthrough/WS endpoint).
// Design: 02-contracts.md §1/§2/§5/§9.2/§10, ADR-010.
package proxy
