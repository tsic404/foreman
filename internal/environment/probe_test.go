package environment_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/environment"
)

func probeScript(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "probe.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
		t.Fatalf("write probe script: %v", err)
	}
	return path
}

// The helper receives the target as one JSON document in its environment and
// answers the verified facts on stdout; operator input never reaches a shell.
func TestExecProbePassesTheTargetAndParsesTheAnswer(t *testing.T) {
	capture := filepath.Join(t.TempDir(), "target.json")
	path := probeScript(t, `
printf '%s' "$FOREMAN_ENVIRONMENT_TARGET" > "$CAPTURE"
printf '%s' '{"tree_id":"tree-1","identity":"sha256:one","capabilities":["nix","docker"],"capacity":8,"version":"rsbxd-1"}'
`)
	probe := environment.ExecProbe{Path: path, Env: []string{"CAPTURE=" + capture}}
	result, err := probe.Inspect(context.Background(), environment.Target{
		Endpoint:       "build-a.example.test:7443",
		TLSRef:         "ref/build-a",
		StateRoot:      "/srv/rsbx/build-a",
		TrustDomain:    "build",
		AllowedSources: []string{"atlas"},
		Capabilities:   []string{"nix"},
		Concurrency:    4,
	})
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if result.TreeID != "tree-1" || result.Identity != "sha256:one" || result.Capacity != 8 ||
		result.Version != "rsbxd-1" || len(result.Capabilities) != 2 {
		t.Fatalf("result = %+v, want the helper's facts", result)
	}
	raw, err := os.ReadFile(capture)
	if err != nil {
		t.Fatalf("read captured target: %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("captured target is not JSON: %v (%s)", err, raw)
	}
	if document["endpoint"] != "build-a.example.test:7443" || document["state_root"] != "/srv/rsbx/build-a" ||
		document["trust_domain"] != "build" || document["concurrency"] != float64(4) {
		t.Fatalf("captured target = %+v, want the operator's registration", document)
	}
}

func TestExecProbeRejectsBadHelperAnswers(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{name: "not JSON", body: `printf 'not-json'`, want: "expected JSON"},
		{name: "unknown field", body: `printf '%s' '{"tree_id":"tree-1","identity":"i","capabilities":[],"capacity":1,"version":"v","extra":true}'`, want: "expected JSON"},
		{name: "trailing JSON", body: `printf '%s' '{"tree_id":"tree-1","identity":"i","capabilities":[],"capacity":1,"version":"v"} {"tree_id":"tree-2"}'`, want: "trailing JSON"},
		{name: "nonzero exit", body: `exit 7`, want: "status 7"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			probe := environment.ExecProbe{Path: probeScript(t, tc.body)}
			if _, err := probe.Inspect(context.Background(), environment.Target{}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Inspect = %v, want an error mentioning %q", err, tc.want)
			}
		})
	}
}

func TestExecProbeHonoursTheCallerDeadline(t *testing.T) {
	probe := environment.ExecProbe{Path: probeScript(t, `sleep 2`)}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := probe.Inspect(ctx, environment.Target{})
	if err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("Inspect = %v, want the deadline named", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Inspect took %s, want it to stop with the context", elapsed)
	}
}

func TestExecProbeRejectsAnEmptyPath(t *testing.T) {
	if _, err := (environment.ExecProbe{Path: "  "}).Inspect(context.Background(), environment.Target{}); err == nil {
		t.Fatal("Inspect accepted an empty helper path")
	}
}

func TestUnavailableProbeFailsClosed(t *testing.T) {
	if _, err := (environment.UnavailableProbe{}).Inspect(context.Background(), environment.Target{}); err == nil ||
		!strings.Contains(err.Error(), "no environment probe") {
		t.Fatalf("UnavailableProbe = %v, want the default reason", err)
	}
	if _, err := (environment.UnavailableProbe{Reason: "not mounted"}).Inspect(context.Background(), environment.Target{}); err == nil ||
		!strings.Contains(err.Error(), "not mounted") {
		t.Fatalf("UnavailableProbe = %v, want the configured reason", err)
	}
}
