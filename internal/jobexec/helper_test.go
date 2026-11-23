package jobexec

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// TestDecodeRequestRefusesAnUnfrozenShape pins the adapter's input contract:
// the supervisor and the adapters ship together, so a field the adapter does
// not know is a version mismatch, not something to ignore.
func TestDecodeRequestRefusesAnUnfrozenShape(t *testing.T) {
	var request struct {
		ClaimID string `json:"claim_id"`
	}
	for _, tc := range []struct {
		name string
		body string
		ok   bool
	}{
		{name: "exact", body: `{"claim_id":"c-1"}`, ok: true},
		{name: "empty", body: `{}`, ok: true},
		{name: "unknown field", body: `{"claim_id":"c-1","extra":1}`},
		{name: "trailing document", body: `{"claim_id":"c-1"}{"claim_id":"c-2"}`},
		{name: "not an object", body: `[]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := decodeRequest(json.RawMessage(tc.body), &request)
			if tc.ok && err != nil {
				t.Fatalf("decodeRequest(%s) = %v, want the frozen shape", tc.body, err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("decodeRequest(%s) = nil, want a refusal", tc.body)
			}
		})
	}
}

// TestServeReportsPhaseFailuresAsExitStatus pins the two answers the supervisor
// distinguishes: a proven "no session was created" status, and every other
// failure, which leaves the creation result unknown.
func TestServeReportsPhaseFailuresAsExitStatus(t *testing.T) {
	phases := map[string]PhaseFunc{
		"ok": func(_ context.Context, _ json.RawMessage) (any, error) { return map[string]string{"a": "1"}, nil },
		"not_found": func(_ context.Context, _ json.RawMessage) (any, error) {
			return nil, Exit(ExitSessionNotCreated, errors.New("no session"))
		},
		"failed": func(_ context.Context, _ json.RawMessage) (any, error) { return nil, errors.New("boom") },
	}
	for _, tc := range []struct {
		name     string
		args     []string
		request  string
		wantExit int
		wantOut  string
	}{
		{name: "ok", args: []string{"ok"}, request: `{}`, wantExit: 0, wantOut: `{"a":"1"}`},
		{name: "no session", args: []string{"not_found"}, request: `{}`, wantExit: ExitSessionNotCreated},
		{name: "other failure", args: []string{"failed"}, request: `{}`, wantExit: 1},
		{name: "unknown phase", args: []string{"nope"}, request: `{}`, wantExit: 2},
		{name: "no phase", args: nil, request: `{}`, wantExit: 2},
		{name: "no request", args: []string{"ok"}, request: "", wantExit: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := Serve(tc.args, tc.request, phases, &stdout, &stderr)
			if code != tc.wantExit {
				t.Fatalf("exit = %d, want %d (stderr %q)", code, tc.wantExit, stderr.String())
			}
			if tc.wantOut != "" && strings.TrimSpace(stdout.String()) != tc.wantOut {
				t.Fatalf("stdout = %q, want %q", stdout.String(), tc.wantOut)
			}
		})
	}
}
