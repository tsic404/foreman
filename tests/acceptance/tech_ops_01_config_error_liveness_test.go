// TC-tech-ops-01 (AC-65, AC-66, AC-86): the config-error face fails the process
// explicitly and the health endpoint's liveness face follows the critical
// loops. A controlled upstream answers the workspace credential check (C-17)
// with 403 — the credential/identity mismatch §1.5 places at the startup
// self-check, never a per-response-code verdict at runtime (ADR-049: runtime
// 401/403 backs off and retries, AC-86). The check is not retried: one request,
// the loop stops, the component.failed event names it with the error summary,
// and the supervisor hands main the error it exits non-zero on. In the
// serviceable window before the other loops drain, /foreman/healthz answers
// 503 and lists the stopped component; while every loop runs it answers 200.
//
// The health/supervisor composition is the production one: the supervisor,
// the liveness view and the ops handler are the packages cmd/foreman wires.
package acceptance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/lifecycle"
	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/proxy"
)

// ops01Healthz reads the liveness endpoint and decodes its body.
func ops01Healthz(t *testing.T, base string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(base + "/foreman/healthz")
	if err != nil {
		t.Fatalf("GET /foreman/healthz: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode /foreman/healthz: %v", err)
	}
	return resp.StatusCode, body
}

func TestTechOps01ConfigErrorFailsTheProcessAndHealthReportsIt(t *testing.T) {
	var calls atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/daemon/workspaces", func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"token does not match daemon_id"}`))
	})
	upstream := httptest.NewServer(mux)
	t.Cleanup(upstream.Close)

	client := proxy.NewClient(proxy.Config{
		ServerURL:   upstream.URL,
		WorkspaceID: "ws-1",
		DaemonID:    "foreman",
		Version:     "0.1.0",
	}, "mdt_ws-1")

	health := observability.NewHealth()
	// The probe face the deployment mounts on the Job listener: the liveness
	// verdict alone, readable without the ops credential a kubelet probe
	// cannot carry (§2.2 S-25).
	opsSrv := httptest.NewServer(observability.NewHealthHandler(health, time.Now()))
	t.Cleanup(opsSrv.Close)

	logs := &syncBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sup := lifecycle.New(slog.New(slog.NewJSONHandler(logs, nil)), health, cancel)

	trigger := make(chan struct{})
	release := make(chan struct{})
	// The critical loops stay running across the failure: they hold the
	// serviceable window the TC reads the endpoint in.
	for _, name := range []string{"claim/ws-1", "heartbeat/ws-1", "lease/ws-1"} {
		sup.Run(ctx, name, func(context.Context) error {
			<-release
			return nil
		})
	}
	// The credential check is the failing critical component. Its C-17 request
	// is the controlled 403.
	sup.Run(ctx, "workspace-credential/ws-1", func(ctx context.Context) error {
		<-trigger
		return client.ValidateWorkspaceScope(ctx)
	})

	// Normal start: every loop runs, the endpoint answers available (200).
	if status, body := ops01Healthz(t, opsSrv.URL); status != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("healthz while every loop runs = %d %+v, want 200 ok", status, body)
	}

	close(trigger)
	select {
	case <-ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("the 403 config error must stop the process instead of retrying")
	}

	// AC-65: the 403 is not retried, the failure names the component and
	// carries the error summary, and it is the value main exits non-zero on.
	if got := calls.Load(); got != 1 {
		t.Fatalf("C-17 calls = %d, want 1 (a configuration error is not retried)", got)
	}
	err := sup.Failure()
	if err == nil {
		t.Fatal("the component failure produced no process-level error")
	}
	if !strings.Contains(err.Error(), "workspace-credential/ws-1") || !strings.Contains(err.Error(), "403") {
		t.Errorf("process-level error = %v, want the component name and the 403 summary", err)
	}
	if !proxy.IsCredentialConfigError(err) {
		t.Errorf("error %v is not classified as the credential configuration error", err)
	}
	var failedEvent bool
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var event map[string]any
		if json.Unmarshal([]byte(line), &event) != nil {
			continue
		}
		if event["msg"] != "component.failed" {
			continue
		}
		if event["component"] != "workspace-credential/ws-1" {
			t.Errorf("component.failed names %v, want workspace-credential/ws-1", event["component"])
			continue
		}
		if summary := fmt.Sprint(event["err"]); !strings.Contains(summary, "403") {
			t.Errorf("component.failed err = %q, want the 403 summary", summary)
		}
		failedEvent = true
	}
	if !failedEvent {
		t.Fatalf("no component.failed event naming the component:\n%s", logs.String())
	}

	// The stopped marker is written by the component goroutine's defer after
	// the supervisor cancels the root context: poll for it before reading the
	// endpoint, so the 503 assertion does not depend on scheduling.
	stoppedDeadline := time.Now().Add(2 * time.Second)
	for len(health.Stopped()) == 0 && time.Now().Before(stoppedDeadline) {
		time.Sleep(time.Millisecond)
	}
	if got := health.Stopped(); len(got) != 1 || got[0] != "workspace-credential/ws-1" {
		t.Fatalf("health stopped = %v, want [workspace-credential/ws-1]", got)
	}

	// AC-66: before the process exits, the endpoint answers unavailable and
	// lists the stopped loop while the other loops still run.
	status, body := ops01Healthz(t, opsSrv.URL)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("healthz after the loop stopped = %d, want 503", status)
	}
	if body["status"] != "failed" {
		t.Errorf("healthz status = %v, want failed", body["status"])
	}
	stopped, _ := body["stopped_components"].([]any)
	if len(stopped) != 1 || stopped[0] != "workspace-credential/ws-1" {
		t.Errorf("stopped_components = %v, want [workspace-credential/ws-1]", body["stopped_components"])
	}

	close(release)
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer drainCancel()
	if !sup.Wait(drainCtx) {
		t.Fatal("the loops did not drain")
	}
}

// ops01FreePort reserves the Job-face address the started process binds: the
// probe is read over TCP, so the port must be known before the run starts.
func ops01FreePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a Job-face port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("release the reserved port: %v", err)
	}
	return port
}

// ops01Binary builds cmd/foreman with the Job template carrier pointed at the
// test-owned reference template. The carrier is a compiled-in deployment path
// (§8.1): a test run can neither write /etc nor mount a private one over it, so
// this one constant is rewritten through a build overlay. Everything else —
// the ops gate under test included — is the production build.
func ops01Binary(t *testing.T, template string) string {
	t.Helper()
	root := filepath.Join("..", "..")
	source := filepath.Join(root, "internal", "jobbuilder", "template.go")
	raw, err := os.ReadFile(source)
	if err != nil {
		t.Fatalf("read the template carrier source: %v", err)
	}
	carrier := regexp.MustCompile(`(?m)^const TemplatePath = ".*"$`)
	if !carrier.Match(raw) {
		t.Fatalf("%s no longer declares the template carrier path", source)
	}
	patched := carrier.ReplaceAll(raw, []byte("const TemplatePath = "+strconv.Quote(template)))
	absolute, err := filepath.Abs(source)
	if err != nil {
		t.Fatalf("resolve the carrier source path: %v", err)
	}
	if resolved, err := filepath.EvalSymlinks(absolute); err == nil {
		absolute = resolved
	}
	dir := t.TempDir()
	patchedPath := filepath.Join(dir, "template.go")
	if err := os.WriteFile(patchedPath, patched, 0o644); err != nil {
		t.Fatalf("write the patched carrier source: %v", err)
	}
	overlay, err := json.Marshal(map[string]map[string]string{
		"Replace": {absolute: patchedPath},
	})
	if err != nil {
		t.Fatalf("encode the build overlay: %v", err)
	}
	overlayPath := filepath.Join(dir, "overlay.json")
	if err := os.WriteFile(overlayPath, overlay, 0o644); err != nil {
		t.Fatalf("write the build overlay: %v", err)
	}

	bin := filepath.Join(t.TempDir(), "foreman")
	build := exec.Command("go", "build", "-overlay", overlayPath, "-o", bin, "./cmd/foreman")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build ./cmd/foreman over the template carrier: %v\n%s", err, out)
	}
	return bin
}

// ops01GateEnv is the deployment environment both gate cases start from: a
// stub kubeconfig (built but never used for a call that decides the verdict), a
// writable state root, and the workspace the controlled server answers for.
func ops01GateEnv(t *testing.T) map[string]string {
	t.Helper()
	return map[string]string{
		"KUBECONFIG":           writeStubKubeconfig(t),
		"FOREMAN_STATE_ROOT":   t.TempDir(),
		"MULTICA_WORKSPACE_ID": "ws-1",
		"MULTICA_TOKEN":        "mdt_ws-1",
	}
}

// TestTechOps01OpsCredentialGate holds the ops-surface startup gate (AC-86):
// an enabled ops listener without any ops credential refuses startup, and the
// same deployment with the listener switched off starts and serves. Both are
// judged on the process — exit code, stderr text, and the Job face answering.
func TestTechOps01OpsCredentialGate(t *testing.T) {
	bin := ops01Binary(t, deployedTemplatePath(t))

	t.Run("no ops credential with the ops listener refuses startup", func(t *testing.T) {
		upstream := newUpstreamStub(t)
		// A fixed Job-face port makes the health probe readable: the refused
		// process never binds it, so a 200 anywhere in the window is the
		// failure AC-65 names.
		port := ops01FreePort(t)
		base := fmt.Sprintf("http://127.0.0.1:%d", port)
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()

		cmd := exec.CommandContext(ctx, bin, fmt.Sprintf("-listen=127.0.0.1:%d", port), "-ops-listen=127.0.0.1:0")
		cmd.Env = foremanStartupEnv(upstream.server.URL, ops01GateEnv(t))
		var stdout, stderr strings.Builder
		cmd.Stdout, cmd.Stderr = &stdout, &stderr

		var served atomic.Bool
		probeCtx, stopProbe := context.WithCancel(context.Background())
		probeDone := make(chan struct{})
		go func() {
			defer close(probeDone)
			for {
				if status, _, err := ops01Probe(base); err == nil && status == http.StatusOK {
					served.Store(true)
					return
				}
				select {
				case <-probeCtx.Done():
					return
				default:
				}
				time.Sleep(2 * time.Millisecond)
			}
		}()
		err := cmd.Run()
		stopProbe()
		<-probeDone
		if ctx.Err() != nil {
			t.Fatalf("foreman did not exit within the window; stderr:\n%s", stderr.String())
		}
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("run foreman = %v, want a non-zero exit; stderr:\n%s", err, stderr.String())
		}
		// A run that stopped at the template gate never reaches the ops gate:
		// say so instead of reporting the ops credential as unmet.
		if strings.Contains(stderr.String(), jobbuilder.TemplatePath) {
			t.Fatalf("the build overlay did not reach the template carrier; stderr:\n%s", stderr.String())
		}
		if code := exitErr.ExitCode(); code != 1 {
			t.Fatalf("exit code = %d, want 1; stderr:\n%s", code, stderr.String())
		}
		// The error names the missing configuration items...
		for _, missing := range []string{observability.EnvOpsToken, observability.EnvOpsWorkspaceTokens} {
			if !strings.Contains(stderr.String(), missing) {
				t.Errorf("stderr does not name the missing configuration item %s:\n%s", missing, stderr.String())
			}
		}
		// ...and never echoes a configured value.
		for _, secret := range []string{"mdt_ws-1", strings.Repeat("ab", 32)} {
			if strings.Contains(stdout.String(), secret) || strings.Contains(stderr.String(), secret) {
				t.Errorf("startup output echoes the configured value %q\nstdout:\n%s\nstderr:\n%s",
					secret, stdout.String(), stderr.String())
			}
		}
		// Nothing serves: the refused process never reaches registration,
		// heartbeat, claim or the subscription handshake.
		for _, path := range []string{"/api/daemon/register", "/api/daemon/heartbeat", "/api/daemon/tasks/claim"} {
			if calls := upstream.pathCalls(path); len(calls) != 0 {
				t.Errorf("%s reached the server %d times during a refused startup, want 0", path, len(calls))
			}
		}
		if dials := upstream.wsDialCount(); dials != 0 {
			t.Errorf("the subscription handshake ran %d times during a refused startup, want 0", dials)
		}
		// AC-65: the health endpoint never answered available...
		if served.Load() {
			t.Errorf("the health endpoint answered available during a refused startup")
		}
		// ...and the Job face never held its port.
		listener, lerr := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if lerr != nil {
			t.Errorf("the Job-face port is still held after the refused startup: %v", lerr)
		} else {
			_ = listener.Close()
		}
	})

	t.Run("ops listener disabled starts and serves", func(t *testing.T) {
		upstream := newUpstreamStub(t)
		port := ops01FreePort(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		// The same deployment shape as the refusal above, minus the listener:
		// the missing credential is not an error when the surface is off.
		cmd := exec.CommandContext(ctx, bin, fmt.Sprintf("-listen=127.0.0.1:%d", port), "-ops-listen=")
		cmd.Env = foremanStartupEnv(upstream.server.URL, ops01GateEnv(t))
		var stdout, stderr strings.Builder
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("start foreman: %v", err)
		}
		exited := make(chan error, 1)
		go func() { exited <- cmd.Wait() }()

		base := fmt.Sprintf("http://127.0.0.1:%d", port)
		deadline := time.Now().Add(20 * time.Second)
		for {
			select {
			case err := <-exited:
				if strings.Contains(stderr.String(), "address already in use") {
					t.Fatalf("the Job-face port was taken between reservation and start (an address race, not a verdict): %v", err)
				}
				t.Fatalf("foreman exited before serving with the ops listener disabled: %v\nstdout:\n%s\nstderr:\n%s",
					err, stdout.String(), stderr.String())
			default:
			}
			if status, body, err := ops01Probe(base); err == nil && status == http.StatusOK && body["status"] == "ok" {
				break
			}
			if time.Now().After(deadline) {
				cancel()
				<-exited
				t.Fatalf("Job face did not answer available within the window\nstdout:\n%s\nstderr:\n%s",
					stdout.String(), stderr.String())
			}
			time.Sleep(20 * time.Millisecond)
		}
		// The other half of the contract: with the surface off, the ops port is
		// not listening — the Job face is the process's only listener.
		if got := ops01ListenPorts(t, cmd.Process.Pid); len(got) != 1 || got[0] != port {
			t.Errorf("listening ports = %v, want only the Job face %d", got, port)
		}
		// ...and the ops data plane is not reachable through that listener.
		resp, err := ops01Client.Get(base + "/foreman/tasks")
		if err != nil {
			t.Fatalf("GET /foreman/tasks on the Job face: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("Job face /foreman/tasks = %d, want 404: the ops data plane lives on the ops listener alone",
				resp.StatusCode)
		}
		cancel()
		<-exited
	})
}

// ops01ListenPorts lists the TCP ports the process holds in LISTEN. The
// /proc/<pid>/net tables belong to the network namespace, so the rows are
// narrowed to the socket inodes the process owns: "the ops port is not
// listening" is judged on this set, not on a log line.
func ops01ListenPorts(t *testing.T, pid int) []int {
	t.Helper()
	fds, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil {
		t.Fatalf("read the process file descriptors: %v", err)
	}
	owned := make(map[string]bool, len(fds))
	for _, fd := range fds {
		target, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", pid, fd.Name()))
		if err != nil || !strings.HasPrefix(target, "socket:[") {
			continue
		}
		owned[strings.TrimSuffix(strings.TrimPrefix(target, "socket:["), "]")] = true
	}
	var ports []int
	for _, table := range []string{"tcp", "tcp6"} {
		raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/net/%s", pid, table))
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(raw), "\n")[1:] {
			fields := strings.Fields(line)
			// sl local_address rem_address st ... inode: state 0A is LISTEN.
			if len(fields) < 10 || fields[3] != "0A" || !owned[fields[9]] {
				continue
			}
			_, hexPort, ok := strings.Cut(fields[1], ":")
			if !ok {
				continue
			}
			port, err := strconv.ParseUint(hexPort, 16, 16)
			if err != nil {
				continue
			}
			ports = append(ports, int(port))
		}
	}
	sort.Ints(ports)
	return ports
}

// ops01Client bounds every probe: a stalled accept must fail the poll instead
// of holding the package until the test timeout.
var ops01Client = &http.Client{Timeout: 500 * time.Millisecond}

// ops01Probe reads the Job face's liveness endpoint while the process runs.
func ops01Probe(base string) (int, map[string]any, error) {
	resp, err := ops01Client.Get(base + "/foreman/healthz")
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, body, nil
}
