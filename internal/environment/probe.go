package environment

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

// ProbeResult is the verified target facts. TreeID is the persistent tree's
// stable identity; the endpoint is only its current address.
type ProbeResult struct {
	TreeID       string   `json:"tree_id"`
	Identity     string   `json:"identity"`
	Capabilities []string `json:"capabilities"`
	Capacity     int      `json:"capacity"`
	Version      string   `json:"version"`
}

// Probe inspects one target through the deployment's controlled path. It is
// read-only: verification never installs software or deletes remote data.
type Probe interface {
	Inspect(ctx context.Context, target Target) (ProbeResult, error)
}

// UnavailableProbe is the honest answer when the deployment has not supplied a
// probe helper. Registrations are stored but stay unverified.
type UnavailableProbe struct {
	Reason string
}

// Inspect always fails with the configured reason.
func (p UnavailableProbe) Inspect(context.Context, Target) (ProbeResult, error) {
	reason := strings.TrimSpace(p.Reason)
	if reason == "" {
		reason = "no environment probe is configured"
	}
	return ProbeResult{}, errors.New(reason)
}

// ExecProbe runs the deployment's fixed probe helper. The helper receives the
// target as one JSON environment variable and answers the verified facts on
// stdout. It is a path, not a shell command: operator input never reaches a
// shell.
type ExecProbe struct {
	Path string
	// Env is the helper's environment. Nil inherits the process environment.
	Env []string
}

const (
	probeTargetEnv = "FOREMAN_ENVIRONMENT_TARGET"
	maxProbeStdout = 64 << 10
	maxProbeStderr = 8 << 10
)

type probeTargetDocument struct {
	Endpoint       string   `json:"endpoint"`
	TLSRef         string   `json:"tls_ref"`
	StateRoot      string   `json:"state_root"`
	TrustDomain    string   `json:"trust_domain"`
	AllowedSources []string `json:"allowed_sources"`
	Capabilities   []string `json:"capabilities"`
	Concurrency    int      `json:"concurrency"`
}

// Inspect runs the helper once under the caller's context.
func (p ExecProbe) Inspect(ctx context.Context, target Target) (ProbeResult, error) {
	if strings.TrimSpace(p.Path) == "" {
		return ProbeResult{}, errors.New("the environment probe path is empty")
	}
	document, err := json.Marshal(probeTargetDocument{
		Endpoint:       target.Endpoint,
		TLSRef:         target.TLSRef,
		StateRoot:      target.StateRoot,
		TrustDomain:    target.TrustDomain,
		AllowedSources: target.AllowedSources,
		Capabilities:   target.Capabilities,
		Concurrency:    target.Concurrency,
	})
	if err != nil {
		return ProbeResult{}, fmt.Errorf("encode the probe target: %w", err)
	}
	stdout := &boundedBuffer{limit: maxProbeStdout}
	stderr := &boundedBuffer{limit: maxProbeStderr}
	cmd := exec.CommandContext(ctx, p.Path)
	// A helper that leaves a child holding the output pipe must not keep the
	// verification past its deadline. WaitDelay closes the pipes shortly after
	// the context ends instead of waiting for the whole process tree.
	cmd.WaitDelay = 500 * time.Millisecond
	cmd.Env = append(append([]string{}, p.Env...), probeTargetEnv+"="+string(document))
	if p.Env == nil {
		cmd.Env = append(os.Environ(), probeTargetEnv+"="+string(document))
	}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return ProbeResult{}, fmt.Errorf("probe did not finish before the deadline")
		}
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return ProbeResult{}, fmt.Errorf("probe exited with status %d", exit.ExitCode())
		}
		return ProbeResult{}, fmt.Errorf("run the environment probe: %w", err)
	}
	var result ProbeResult
	decoder := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return ProbeResult{}, errors.New("probe output is not the expected JSON")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ProbeResult{}, errors.New("probe output carries trailing JSON")
	}
	return result, nil
}

// boundedBuffer keeps at most limit bytes and reports truncation without
// growing without bound.
type boundedBuffer struct {
	buf   bytes.Buffer
	limit int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	remaining := b.limit - b.buf.Len()
	if remaining > 0 {
		if len(p) < remaining {
			remaining = len(p)
		}
		_, _ = b.buf.Write(p[:remaining])
	}
	return len(p), nil
}

func (b *boundedBuffer) Bytes() []byte {
	return b.buf.Bytes()
}
