package jobexec

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/attempt"
)

// writeTLSMaterial writes one environment's projected material: a trust
// anchor, a client certificate signed by it, the client key and the expected
// server name.
func writeTLSMaterial(t *testing.T, dir string) (caPath, certPath, keyPath string) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate the CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "foreman-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create the CA certificate: %v", err)
	}
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate the client key: %v", err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "foreman-job"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caTemplate, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create the client certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(clientKey)
	if err != nil {
		t.Fatalf("marshal the client key: %v", err)
	}
	caPath = filepath.Join(dir, tlsCAFile)
	certPath = filepath.Join(dir, tlsCertFile)
	keyPath = filepath.Join(dir, tlsKeyFile)
	writePEM(t, caPath, "CERTIFICATE", caDER)
	writePEM(t, certPath, "CERTIFICATE", clientDER)
	writePEM(t, keyPath, "EC PRIVATE KEY", keyDER)
	if err := os.WriteFile(filepath.Join(dir, tlsServerNameFile), []byte("rsbxd.example.test\n"), 0o600); err != nil {
		t.Fatalf("write the server name: %v", err)
	}
	return caPath, certPath, keyPath
}

func writePEM(t *testing.T, path, kind string, der []byte) {
	t.Helper()
	body := pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der})
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// probeAwareCarrier answers the adapter's one-shot ready probe by executing
// the argv it was given, and keeps a long-lived session alive. rsbx has no
// separate probe mode: the adapter runs a bounded command through the same
// carrier before it starts the session.
const probeAwareCarrier = `#!/bin/sh
if [ "$1" = "-config" ]; then shift 2; fi
if [ "$1" = "/bin/sh" ]; then shift; exec /bin/sh "$@"; fi
exec sleep 60
`

// adapterEnv builds one isolated adapter environment: a Job credential, a
// private session root and a fake carrier that is a real executable.
func adapterEnv(t *testing.T, carrier string) map[string]string {
	t.Helper()
	dir := t.TempDir()
	credential := filepath.Join(dir, "config.json")
	if err := os.WriteFile(credential, []byte(`{"token":"fmj_test"}`), 0o400); err != nil {
		t.Fatalf("write the Job credential: %v", err)
	}
	clis := filepath.Join(dir, "clis")
	if err := os.MkdirAll(clis, 0o755); err != nil {
		t.Fatalf("create the CLI directory: %v", err)
	}
	home := filepath.Join(dir, "home")
	if err := os.MkdirAll(home, 0o755); err != nil {
		t.Fatalf("create the Job home: %v", err)
	}
	entry := filepath.Join(dir, "entry")
	writeExecutable(t, entry, "#!/bin/sh\nexec sleep 60\n")
	carrierPath := filepath.Join(dir, "rsbx")
	writeExecutable(t, carrierPath, carrier)
	// Ready cross-checks the frozen binding against the environment's verified
	// identity, so every adapter environment needs a control plane that answers
	// the matching facts by default. Tests that need other facts override
	// EnvControlURL with their own server.
	control := controlServer(t, func() map[string]any {
		return map[string]any{
			"attempt":     map[string]any{"task_id": "task-1"},
			"environment": map[string]any{"tree_id": "generation-1", "identity": "identity-1", "state": "ok"},
		}
	})
	// The session root must leave room for the relay socket's AF_UNIX path
	// (107 bytes), so the fixture allocates its own short one.
	sessionRoot := shortTempDir(t, "fes-")
	return map[string]string{
		EnvTokenFile:     credential,
		EnvCLIDir:        clis,
		EnvSessionDir:    filepath.Join(sessionRoot, "s"),
		EnvRsbxBinary:    carrierPath,
		EnvRsbxEntry:     `["` + entry + `"]`,
		EnvControlURL:    control.URL,
		EnvSessionSettle: "150ms",
		"HOME":           home,
	}
}

func writeExecutable(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// shortTempDir allocates a directory whose path leaves room for the relay
// socket's AF_UNIX address. The test binary's own TMPDIR may be long enough
// to spend the whole 107-byte budget, so the fixture prefers the shortest
// usable base and falls back to the default only when it is already short.
func shortTempDir(t *testing.T, prefix string) string {
	t.Helper()
	base := os.TempDir()
	if info, err := os.Stat("/tmp"); err == nil && info.IsDir() && len("/tmp") < len(base) {
		base = "/tmp"
	}
	dir, err := os.MkdirTemp(base, prefix)
	if err != nil {
		t.Fatalf("create a short session root under %s: %v", base, err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// withEnv applies one adapter environment and restores the process's own after
// the test. The adapters read the process environment by design: they run as
// one short-lived process inside the Job.
func withEnv(t *testing.T, env map[string]string) {
	t.Helper()
	for key, value := range env {
		t.Setenv(key, value)
	}
}

func testBinding(t *testing.T) attempt.Binding {
	t.Helper()
	return attempt.Binding{
		AttemptID: "attempt-1", TaskID: "task-1", WorkspaceID: "ws-1", AgentID: "agent-1",
		DaemonID: "fm-task-1", JobRuntimeID: "runtime-1", EnvironmentID: "env-1",
		Endpoint: "/run/rsbx/daemon.sock", TLSRef: "ref/env-1", StateRoot: t.TempDir(),
		EnvironmentGen: "generation-1", EnvironmentIdentity: "identity-1", SlotID: "slot-1",
	}
}

func digestOfBody(body []byte) string {
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func phase(t *testing.T, fn PhaseFunc, request any) (any, error) {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("encode the phase request: %v", err)
	}
	return fn(context.Background(), body)
}

// TestReadyReportsAProvableFailureAsNoSession pins the one exit status the
// supervisor treats as "nothing was created": a carrier that exits during its
// own preflight, before any entry starts.
func TestReadyReportsAProvableFailureAsNoSession(t *testing.T) {
	withEnv(t, adapterEnv(t, "#!/bin/sh\necho 'daemon unreachable' >&2\nexit 1\n"))
	_, err := phase(t, Ready, readyRequest{Binding: testBinding(t)})
	if err == nil {
		t.Fatal("Ready accepted a carrier that never reached its daemon")
	}
	var exit *ExitError
	if !errors.As(err, &exit) || exit.Code != ExitSessionNotCreated {
		t.Fatalf("Ready error = %v, want exit %d: a carrier that exited before any entry proves no session", err, ExitSessionNotCreated)
	}
}

// TestReadyAnswersOnlyAfterTheCarrierProbe pins the happy path: the identity
// is stable for the frozen binding, and the close stays unknown because the
// carrier has no remote close protocol.
func TestReadyAnswersOnlyAfterTheCarrierProbe(t *testing.T) {
	withEnv(t, adapterEnv(t, probeAwareCarrier))
	binding := testBinding(t)

	answer, err := phase(t, Ready, readyRequest{Binding: binding})
	if err != nil {
		t.Fatalf("Ready: %v", err)
	}
	session, ok := answer.(attempt.Session)
	if !ok {
		t.Fatalf("Ready answered %T, want the session identity", answer)
	}
	if session.ID != SessionID(binding) {
		t.Errorf("session id = %q, want the frozen binding's own %q", session.ID, SessionID(binding))
	}
	if session.EnvironmentID != binding.EnvironmentID || session.Generation != binding.EnvironmentGen || session.Endpoint != binding.Endpoint {
		t.Errorf("session = %+v, want the frozen binding's environment/generation/endpoint", session)
	}
	if session.ID == SessionID(attempt.Binding{AttemptID: "other", EnvironmentID: binding.EnvironmentID,
		EnvironmentGen: binding.EnvironmentGen, Endpoint: binding.Endpoint}) {
		t.Error("two attempts on one environment answered the same session id")
	}

	answer, err = phase(t, Close, closeRequest{Session: session})
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	evidence, ok := answer.(attempt.CloseEvidence)
	if !ok {
		t.Fatalf("Close answered %T, want close evidence", answer)
	}
	if evidence.Confirmed {
		t.Fatal("Close confirmed a remote close the carrier never reported")
	}
	if strings.TrimSpace(evidence.Detail) == "" {
		t.Error("Close confirmed nothing and said nothing about it")
	}
}

// TestReadyRefusesAHangingProbe pins the negative side: a carrier that
// connects but never answers is not ready, and no session is created.
func TestReadyRefusesAHangingProbe(t *testing.T) {
	env := adapterEnv(t, "#!/bin/sh\nexec sleep 60\n")
	env[EnvReadyTimeout] = "250ms"
	withEnv(t, env)
	_, err := phase(t, Ready, readyRequest{Binding: testBinding(t)})
	if err == nil {
		t.Fatal("Ready answered a carrier that never completed the probe")
	}
	var exit *ExitError
	if !errors.As(err, &exit) || exit.Code != ExitSessionNotCreated {
		t.Fatalf("Ready error = %v, want exit %d", err, ExitSessionNotCreated)
	}
}

// TestReadyRequiresResolvableTLSMaterial pins the TLS half: when the
// deployment configures a trust directory, the frozen reference must resolve
// to a parseable bundle before any session opens.
func TestReadyRequiresResolvableTLSMaterial(t *testing.T) {
	env := adapterEnv(t, probeAwareCarrier)
	tlsDir := t.TempDir()
	env[EnvTLSDir] = tlsDir
	withEnv(t, env)

	if _, err := phase(t, Ready, readyRequest{Binding: testBinding(t)}); err == nil {
		t.Fatal("Ready accepted a TLS reference that resolves to nothing")
	}

	// A file that is not a certificate bundle is refused too.
	ref := filepath.Join(tlsDir, "ref", "env-1")
	if err := os.MkdirAll(filepath.Dir(ref), 0o700); err != nil {
		t.Fatalf("create the TLS directory: %v", err)
	}
	if err := os.WriteFile(ref, []byte("not a certificate\n"), 0o600); err != nil {
		t.Fatalf("write the TLS material: %v", err)
	}
	if _, err := phase(t, Ready, readyRequest{Binding: testBinding(t)}); err == nil {
		t.Fatal("Ready accepted a TLS reference that is not a certificate bundle")
	}
}

// TestReadyRefusesAStaleTreeGeneration pins the identity half: the frozen
// binding may only be answered when a fresh observation of the environment
// still names the same tree. A tree replaced under the same state-root path is
// refused, never echoed from the input.
func TestReadyRefusesAStaleTreeGeneration(t *testing.T) {
	env := adapterEnv(t, probeAwareCarrier)
	server := controlServer(t, func() map[string]any {
		return map[string]any{
			"attempt":     map[string]any{"task_id": "task-1"},
			"environment": map[string]any{"tree_id": "generation-2", "identity": "identity-1", "state": "ok"},
		}
	})
	env[EnvControlURL] = server.URL
	withEnv(t, env)

	assertNoSession(t, Ready, readyRequest{Binding: testBinding(t)})
}

// TestReadyRefusesAMismatchedIdentity pins the TLS/identity half: a bundle
// that parses is not proof that the target is the frozen one. When the
// verified identity differs from the frozen binding the session is refused
// before anything starts.
func TestReadyRefusesAMismatchedIdentity(t *testing.T) {
	env := adapterEnv(t, probeAwareCarrier)
	server := controlServer(t, func() map[string]any {
		return map[string]any{
			"attempt":     map[string]any{"task_id": "task-1"},
			"environment": map[string]any{"tree_id": "generation-1", "identity": "identity-2", "state": "ok"},
		}
	})
	env[EnvControlURL] = server.URL
	tlsDir := t.TempDir()
	env[EnvTLSDir] = tlsDir
	certificate := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(certificate.Close)
	bundle := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate().Raw})
	ref := filepath.Join(tlsDir, "ref", "env-1")
	if err := os.MkdirAll(filepath.Dir(ref), 0o700); err != nil {
		t.Fatalf("create the TLS directory: %v", err)
	}
	if err := os.WriteFile(ref, bundle, 0o600); err != nil {
		t.Fatalf("write the TLS bundle: %v", err)
	}
	withEnv(t, env)

	assertNoSession(t, Ready, readyRequest{Binding: testBinding(t)})
}

// TestReadyRefusesAnUnobservableEnvironment pins the fail-closed rule: with no
// verified environment facts the adapter has nothing to compare, so it refuses
// rather than answering the frozen input as ready.
func TestReadyRefusesAnUnobservableEnvironment(t *testing.T) {
	env := adapterEnv(t, probeAwareCarrier)
	server := controlServer(t, func() map[string]any {
		return map[string]any{"attempt": map[string]any{"task_id": "task-1"}, "environment": nil}
	})
	env[EnvControlURL] = server.URL
	withEnv(t, env)

	assertNoSession(t, Ready, readyRequest{Binding: testBinding(t)})
}

// recordingProbeCarrier records the arguments of its first invocation — the
// ready probe — before executing the command it was given. Later invocations
// (the long-lived session) leave the record alone.
const recordingProbeCarrier = `#!/bin/sh
if [ -n "$FOREMAN_TEST_CARRIER_ARGS" ] && [ ! -f "$FOREMAN_TEST_CARRIER_ARGS" ]; then
	printf '%s\n' "$@" >"$FOREMAN_TEST_CARRIER_ARGS"
fi
if [ "$1" = "-config" ]; then shift 2; fi
if [ "$1" = "/bin/sh" ]; then shift; exec /bin/sh "$@"; fi
exec sleep 60
`

// recordingSessionCarrier records the environment and argv of the session
// invocation only. The adapter runs the same binary for the ready probe, so
// the recorded config name separates the two: the probe reads probe.toml, the
// session rsbx.toml. Everything else matches the probe-aware fixture.
const recordingSessionCarrier = `#!/bin/sh
if [ "$1" = "-config" ]; then
	case "$2" in
	*/rsbx.toml)
		if [ -n "$FOREMAN_TEST_SESSION_ARGS" ]; then printf '%s\n' "$@" >"$FOREMAN_TEST_SESSION_ARGS"; fi
		if [ -n "$FOREMAN_TEST_SESSION_ENV" ]; then env >"$FOREMAN_TEST_SESSION_ENV"; fi
		;;
	esac
	shift 2
fi
if [ "$1" = "/bin/sh" ]; then shift; exec /bin/sh "$@"; fi
exec sleep 60
`

// TestReadyProvesTheRemoteViewWithoutOpeningTheStateRoot pins what the probe
// observes. The registered state root is the deployer's host-side identity,
// verified by the deployer's own probe helper; the carrier's sandbox view
// answers an unmatched path from the daemon's filesystem, where that host path
// need not exist. A probe that asked its sandbox to open the state root would
// refuse a session the carrier can open — the QA failure this test reproduces.
func TestReadyProvesTheRemoteViewWithoutOpeningTheStateRoot(t *testing.T) {
	env := adapterEnv(t, recordingProbeCarrier)
	argsFile := filepath.Join(t.TempDir(), "carrier-args")
	env["FOREMAN_TEST_CARRIER_ARGS"] = argsFile
	withEnv(t, env)

	binding := testBinding(t)
	// The parent exists, the registered root does not: a host path the Job's
	// sandbox cannot open.
	binding.StateRoot = filepath.Join(t.TempDir(), "host-state-root")

	if _, err := phase(t, Ready, readyRequest{Binding: binding}); err != nil {
		t.Fatalf("Ready refused a session over a host-side state root: %v", err)
	}

	args, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatalf("read the carrier arguments: %v", err)
	}
	recorded := string(args)
	if strings.Contains(recorded, "test -d") || strings.Contains(recorded, binding.StateRoot) {
		t.Fatalf("the probe asks its sandbox to open the registered host path:\n%s", recorded)
	}
	if !strings.Contains(recorded, "foreman-ready-") {
		t.Fatalf("the probe carries no marker:\n%s", recorded)
	}

	settings, err := LoadSettings()
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}
	config, err := os.ReadFile(filepath.Join(settings.attemptDir(binding.AttemptID), "probe.toml"))
	if err != nil {
		t.Fatalf("read the rendered probe config: %v", err)
	}
	if strings.Contains(string(config), binding.StateRoot) {
		t.Fatalf("the probe config projects the state root it must not need:\n%s", config)
	}
}

// TestProbeScriptPrintsOnlyTheMarker is the render-side guard: the command the
// probe hands the carrier is one printf, so re-adding a directory check (of
// the state root or anything else) fails here before it fails a cluster run.
func TestProbeScriptPrintsOnlyTheMarker(t *testing.T) {
	if got, want := probeScript("ready-x'y"), `printf '%s' 'ready-x'\''y'`; got != want {
		t.Fatalf("probeScript = %q, want %q: the probe prints the marker and does nothing else", got, want)
	}
}

func assertNoSession(t *testing.T, fn PhaseFunc, request any) {
	t.Helper()
	_, err := phase(t, fn, request)
	if err == nil {
		t.Fatal("the adapter answered a ready it could not prove")
	}
	var exit *ExitError
	if !errors.As(err, &exit) || exit.Code != ExitSessionNotCreated {
		t.Fatalf("error = %v, want exit %d: a refused ready creates no session", err, ExitSessionNotCreated)
	}
}

// TestCloseWithoutARecordStaysUnknown pins the isolation rule: an adapter that
// cannot see the session must not claim it closed, or the ledger would return
// capacity for a remote tree that may still be alive.
func TestCloseWithoutARecordStaysUnknown(t *testing.T) {
	withEnv(t, adapterEnv(t, probeAwareCarrier))
	answer, err := phase(t, Close, closeRequest{Session: attempt.Session{ID: "sbx-unknown", EnvironmentID: "env-1"}})
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	evidence, ok := answer.(attempt.CloseEvidence)
	if !ok {
		t.Fatalf("Close answered %T, want close evidence", answer)
	}
	if evidence.Confirmed {
		t.Fatal("Close confirmed a close it never observed")
	}
	if strings.TrimSpace(evidence.Detail) == "" {
		t.Error("Close confirmed nothing and said nothing about it")
	}
}

// TestCloseNeverClaimsARemoteCloseFromACarrierExit pins the honest answer for
// a carrier without a close protocol: stopping the local process proves the
// connection ended, not that the remote session did, so the record stays.
func TestCloseNeverClaimsARemoteCloseFromACarrierExit(t *testing.T) {
	withEnv(t, adapterEnv(t, probeAwareCarrier))
	answer, err := phase(t, Ready, readyRequest{Binding: testBinding(t)})
	if err != nil {
		t.Fatalf("Ready: %v", err)
	}
	session := answer.(attempt.Session)
	evidence, err := phase(t, Close, closeRequest{Session: session})
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	closed, ok := evidence.(attempt.CloseEvidence)
	if !ok {
		t.Fatalf("Close answered %T, want close evidence", evidence)
	}
	if closed.Confirmed {
		t.Fatalf("Close confirmed a remote close from a local carrier exit: %+v", closed)
	}
	if _, err := resolveSession(mustSettings(t), session); err != nil {
		t.Fatalf("the unconfirmed close removed the session record: %v", err)
	}
}

// TestResolveSessionTargetMapsDeploymentEndpoints pins the endpoint forms the
// reference adapter accepts. An unsupported form is refused before anything
// starts, which is what keeps "no session" provable.
func TestResolveSessionTargetMapsDeploymentEndpoints(t *testing.T) {
	for _, tc := range []struct {
		endpoint  string
		transport string
		socket    string
		address   string
		command   string
		refused   bool
	}{
		{endpoint: "/run/rsbx/daemon.sock", transport: "unix", socket: "/run/rsbx/daemon.sock"},
		{endpoint: "unix:///run/rsbx/daemon.sock", transport: "unix", socket: "/run/rsbx/daemon.sock"},
		{endpoint: "ssh://agent@env.example.test:2222", transport: "ssh", command: "ssh -p 2222 agent@env.example.test rsbxd -stdio"},
		{endpoint: "ssh://env.example.test", transport: "ssh", command: "ssh env.example.test rsbxd -stdio"},
		{endpoint: "cmd:rsbxd -stdio", transport: "ssh", command: "rsbxd -stdio"},
		{endpoint: "relative/socket", refused: true},
		{endpoint: "tcp://env.example.test:7443", refused: true},
		{endpoint: "", refused: true},
		// A bare address is the authenticated network channel: the carrier
		// carries no ssh client. It is refused here only because no material
		// is configured, which is the fail-closed answer.
		{endpoint: "env.example.test:7443", refused: true},
	} {
		t.Run(tc.endpoint, func(t *testing.T) {
			binding := testBinding(t)
			binding.Endpoint = tc.endpoint
			settings := Settings{Home: t.TempDir(), CLIDir: t.TempDir(), SessionDir: t.TempDir()}
			target, err := resolveSessionTarget(binding, settings)
			if tc.refused {
				if err == nil {
					t.Fatalf("resolveSessionTarget(%q) = %+v, want a refusal", tc.endpoint, target)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveSessionTarget(%q): %v", tc.endpoint, err)
			}
			if target.Transport != tc.transport || target.Socket != tc.socket ||
				target.Address != tc.address || target.Command != tc.command {
				t.Errorf("resolveSessionTarget(%q) = %+v, want %q/%q/%q/%q",
					tc.endpoint, target, tc.transport, tc.socket, tc.address, tc.command)
			}
		})
	}
}

// TestResolveSessionTargetRendersAuthenticatedTransport pins the half the
// carrier needs to actually reach a network rsbxd: the transport, the address
// and the client's own certificate and key, not just a trust bundle in the
// process environment.
func TestResolveSessionTargetRendersAuthenticatedTransport(t *testing.T) {
	binding := testBinding(t)
	binding.Endpoint = "tls://env.example.test:7443"
	binding.TLSRef = "ref/env-1"
	settings := Settings{TLSDir: t.TempDir(), SessionDir: t.TempDir()}
	directory := filepath.Join(settings.TLSDir, "ref", "env-1")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatalf("create the TLS root: %v", err)
	}
	ca, cert, key := writeTLSMaterial(t, directory)
	t.Setenv(EnvTLSDir, settings.TLSDir)

	target, err := resolveSessionTarget(binding, settings)
	if err != nil {
		t.Fatalf("resolveSessionTarget: %v", err)
	}
	if target.Transport != "tls" || target.Address != "env.example.test:7443" {
		t.Fatalf("target = %+v, want the authenticated network transport", target)
	}
	if target.TLS.CA != ca || target.TLS.Cert != cert || target.TLS.Key != key {
		t.Fatalf("material = %+v, want %s/%s/%s", target.TLS, ca, cert, key)
	}
	config := renderSessionConfig(binding, settings, target)
	for _, want := range []string{
		`transport = "tls"`,
		`address = "env.example.test:7443"`,
		`tls_ca = "` + ca + `"`,
		`tls_cert = "` + cert + `"`,
		`tls_key = "` + key + `"`,
		`server_name = "rsbxd.example.test"`,
	} {
		if !strings.Contains(config, want) {
			t.Errorf("rendered config lacks %q:\n%s", want, config)
		}
	}
}

// TestResolveSessionTargetRefusesUnusableAuthenticatedMaterial pins the
// fail-closed side: a directory that lacks the key, or a certificate that does
// not match it, is no session rather than a handshake surprise.
func TestResolveSessionTargetRefusesUnusableAuthenticatedMaterial(t *testing.T) {
	directory := t.TempDir()
	writeTLSMaterial(t, directory)
	if err := os.Remove(filepath.Join(directory, tlsKeyFile)); err != nil {
		t.Fatalf("remove the key: %v", err)
	}
	binding := testBinding(t)
	binding.Endpoint = "tls://env.example.test:7443"
	binding.TLSRef = filepath.Join(directory, "..", filepath.Base(directory))
	_, err := resolveSessionTarget(binding, Settings{SessionDir: t.TempDir()})
	if err == nil {
		t.Fatal("a network session opened without the client key")
	}
}

// TestRenderSessionConfigKeepsStateLocalAndCLIsReadOnly pins the carrier
// policy the reference adapter renders: the daemon entry and the Agent CLI it
// starts both run locally, because the daemon's profile (the Job token
// included) and the Agent artifact exist only on the Pod volume. The
// downloaded CLI tree is readable from both sides and never writable.
func TestRenderSessionConfigKeepsStateLocalAndCLIsReadOnly(t *testing.T) {
	settings := Settings{
		CLIDir: "/run/foreman/clis",
		Home:   "/run/foreman/home",
	}
	settings.SessionDir = t.TempDir()
	binding := testBinding(t)
	binding.CLIs = []attempt.CLI{
		{Kind: attempt.KindMultica, Version: "0.11.1", Platform: "linux/amd64"},
		{Kind: attempt.KindAgent, Backend: "omp", Version: "19.0.1", Platform: "linux/amd64"},
	}
	config := renderSessionConfig(binding, settings, sessionTarget{
		Transport: "unix", Socket: "/run/rsbx/daemon.sock",
	})

	for _, want := range []string{
		`transport = "unix"`,
		`socket = "/run/rsbx/daemon.sock"`,
		`entry = "/run/foreman/clis/multica"`,
		`allow = ["/run/foreman/clis/multica", "/run/foreman/clis/agent"]`,
		`paths = ["/run/foreman/home"]`,
		`read_only_paths = ["/run/foreman/clis"]`,
		`mount = "`,
		`unshare_user = false`,
	} {
		if !strings.Contains(config, want) {
			t.Errorf("rendered config lacks %q:\n%s", want, config)
		}
	}
	// The old spelling is the defect: the daemon entry alone is allowed
	// locally, so the Agent the daemon starts is routed to the remote tree and
	// fails with "no agent CLI found". A config that names only the entry must
	// not satisfy this assertion.
	if strings.Contains(config, `allow = ["/run/foreman/clis/multica"]`+"\n") {
		t.Error("the rendered policy allows only the daemon entry: the Agent CLI the daemon starts would run on the remote tree")
	}
	if strings.Contains(config, "\npaths = [\"/run/foreman/clis\"]") {
		t.Error("the downloaded CLI tree is mounted writable: a Job could rewrite its own frozen artifact")
	}
}

// TestRenderSessionConfigNamesTheAgentBackend is the companion guard for the
// second half of the same failure: allowing the artifact locally is not enough
// if the daemon never finds it. The carrier environment names the selected
// backend's launcher path, so the daemon does not have to guess a PATH entry.
func TestRenderSessionConfigNamesTheAgentBackend(t *testing.T) {
	settings := Settings{CLIDir: "/run/foreman/clis", Home: "/run/foreman/home"}
	settings.SessionDir = t.TempDir()

	// A binding without an Agent CLI must not name a path the carrier would
	// then fail to snapshot: the allow list is resolved into existing
	// executables at session start, and a missing entry is a hard refusal.
	bare := renderSessionConfig(testBinding(t), settings, sessionTarget{
		Transport: "unix", Socket: "/run/rsbx/daemon.sock",
	})
	if strings.Contains(bare, "/run/foreman/clis/agent") {
		t.Errorf("a binding without an Agent CLI still names its artifact:\n%s", bare)
	}
	if !strings.Contains(bare, `allow = ["/run/foreman/clis/multica"]`+"\n") {
		t.Errorf("a binding without an Agent CLI dropped its daemon entry:\n%s", bare)
	}

	binding := testBinding(t)
	binding.CLIs = []attempt.CLI{
		{Kind: attempt.KindMultica, Version: "0.11.1", Platform: "linux/amd64"},
		{Kind: attempt.KindAgent, Backend: "omp", Version: "19.0.1", Platform: "linux/amd64"},
	}
	env := sessionEnv(binding, settings, "")
	found := false
	for _, entry := range env {
		if entry == "MULTICA_OMP_PATH=/run/foreman/clis/agent" {
			found = true
		}
	}
	if !found {
		t.Fatalf("the session environment does not name the Agent backend: %v", env)
	}
	// The frozen daemon does not derive every launcher variable the same way:
	// a backend whose command name differs from its variable keeps its own.
	// Pinning the derived spelling here would name a variable the daemon
	// ignores, leaving the Agent undiscoverable.
	for backend, want := range map[string]string{
		"cursor-agent": "MULTICA_CURSOR_PATH",
		"kiro-cli":     "MULTICA_KIRO_PATH",
		"agy":          "MULTICA_ANTIGRAVITY_PATH",
		"qodercli":     "MULTICA_QODER_PATH",
		"codex":        "MULTICA_CODEX_PATH",
	} {
		binding.CLIs[1].Backend = backend
		var got string
		for _, entry := range sessionEnv(binding, settings, "") {
			if strings.HasSuffix(entry, "=/run/foreman/clis/agent") {
				got = strings.SplitN(entry, "=", 2)[0]
			}
		}
		if got != want {
			t.Errorf("backend %q launcher variable = %q, want %q", backend, got, want)
		}
	}
}

// TestStartSessionHandsTheAgentLauncherToTheSessionProcess judges the value
// the session process actually receives, not the helper's own return. Naming
// the launcher in sessionEnv is worth nothing if startSession does not use it:
// the daemon would search PATH and exit with "no agent CLI found".
func TestStartSessionHandsTheAgentLauncherToTheSessionProcess(t *testing.T) {
	env := adapterEnv(t, recordingSessionCarrier)
	envFile := filepath.Join(t.TempDir(), "session-env")
	argsFile := filepath.Join(t.TempDir(), "session-argv")
	env["FOREMAN_TEST_SESSION_ENV"] = envFile
	env["FOREMAN_TEST_SESSION_ARGS"] = argsFile
	withEnv(t, env)

	settings, err := LoadSettings()
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}
	binding := testBinding(t)
	binding.CLIs = []attempt.CLI{
		{Kind: attempt.KindMultica, Version: "0.11.1", Platform: "linux/amd64"},
		{Kind: attempt.KindAgent, Backend: "cursor-agent", Version: "1.2.3", Platform: "linux/amd64"},
	}
	target, err := resolveSessionTarget(binding, settings)
	if err != nil {
		t.Fatalf("resolveSessionTarget: %v", err)
	}

	record, started, err := startSession(binding, settings, target, "generation-1")
	if err != nil || !started {
		t.Fatalf("startSession: started=%v err=%v", started, err)
	}
	t.Cleanup(func() { _ = stopProcess(record.PID, 5*time.Second) })

	// The backend's launcher variable is a known exception: the frozen daemon
	// reads MULTICA_CURSOR_PATH for cursor-agent, not the derived spelling.
	want := "MULTICA_CURSOR_PATH=" + settings.AgentPath()
	if lines := waitForLines(t, envFile); !containsLine(lines, want) {
		t.Fatalf("the session process environment carries no %q; it got %v", want, launcherLines(lines))
	}
	argv := waitForLines(t, argsFile)
	if !containsLine(argv, record.ConfigPath) || !containsLine(argv, "-config") {
		t.Fatalf("the recorded invocation is not the session: %v", argv)
	}
	if containsSubstring(argv, "probe.toml") {
		t.Fatalf("the recorded invocation is the ready probe, not the session: %v", argv)
	}
}

// waitForLines reads a fixture file the carrier writes as it starts, allowing
// for the short window between exec and the file appearing.
func waitForLines(t *testing.T, path string) []string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		body, err := os.ReadFile(path)
		if err == nil && len(body) > 0 {
			return strings.Split(strings.TrimRight(string(body), "\n"), "\n")
		}
		if time.Now().After(deadline) {
			t.Fatalf("read %s: %v", path, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func containsLine(lines []string, want string) bool {
	for _, line := range lines {
		if line == want {
			return true
		}
	}
	return false
}

func containsSubstring(lines []string, want string) bool {
	for _, line := range lines {
		if strings.Contains(line, want) {
			return true
		}
	}
	return false
}

// launcherLines keeps a failure readable: the whole environment is noise.
func launcherLines(lines []string) []string {
	var out []string
	for _, line := range lines {
		if strings.HasPrefix(line, "MULTICA_") && strings.HasSuffix(strings.SplitN(line, "=", 2)[0], "_PATH") {
			out = append(out, line)
		}
	}
	return out
}

// controlServer is the attempt projection the control-facing adapters read.
func controlServer(t *testing.T, snapshot func() map[string]any) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fmj_test" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Method == http.MethodPost {
			_ = json.NewEncoder(w).Encode(map[string]any{"slot": map[string]any{"exists": true, "state": "active"}})
			return
		}
		answer := snapshot()
		if r.URL.Path == ControlPath+"/environment" {
			if fact, ok := answer["environment"]; ok {
				_ = json.NewEncoder(w).Encode(fact)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"tree_id": "generation-1", "identity": "identity-1", "state": "observed"})
			return
		}
		if _, ok := answer["environment"]; !ok {
			answer["environment"] = map[string]any{"tree_id": "generation-1", "identity": "identity-1", "state": "ok"}
		}
		_ = json.NewEncoder(w).Encode(answer)
	}))
	t.Cleanup(server.Close)
	return server
}

// TestRegisterAnswersOnlyAnObservedRegistration pins the observation rule: a
// projection with no registration is never answered as a success, and the
// answer carries exactly what the upstream confirmed.
func TestRegisterAnswersOnlyAnObservedRegistration(t *testing.T) {
	env := adapterEnv(t, probeAwareCarrier)
	var registered atomic.Bool
	server := controlServer(t, func() map[string]any {
		if !registered.Load() {
			return map[string]any{"attempt": map[string]any{"task_id": "task-1"}}
		}
		return map[string]any{
			"attempt": map[string]any{"task_id": "task-1"},
			"registration": map[string]any{
				"daemon_id": "fm-task-1", "runtime_id": "runtime-1", "version": "0.11.1",
			},
		}
	})
	env[EnvControlURL] = server.URL
	env[EnvRegisterTimeout] = "5s"
	withEnv(t, env)

	answer, err := phase(t, Ready, readyRequest{Binding: testBinding(t)})
	if err != nil {
		t.Fatalf("Ready: %v", err)
	}
	session := answer.(attempt.Session)
	// The registration appears only after the adapter has asked once, so a
	// success can only come from an observation.
	go func() {
		time.Sleep(600 * time.Millisecond)
		registered.Store(true)
	}()
	answer, err = phase(t, Register, registerRequest{Binding: testBinding(t), Session: session})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	registration, ok := answer.(attempt.Registration)
	if !ok {
		t.Fatalf("Register answered %T, want the registration fact", answer)
	}
	if registration.DaemonID != "fm-task-1" || registration.RuntimeID != "runtime-1" || registration.Version != "0.11.1" {
		t.Errorf("registration = %+v, want the observed daemon/runtime/version", registration)
	}
}

// TestRegisterNeverAnswersAnUnobservedRegistration pins the failure side: a
// bounded wait that observed nothing is an error, not an empty success.
func TestRegisterNeverAnswersAnUnobservedRegistration(t *testing.T) {
	env := adapterEnv(t, probeAwareCarrier)
	server := controlServer(t, func() map[string]any {
		return map[string]any{"attempt": map[string]any{"task_id": "task-1"}}
	})
	env[EnvControlURL] = server.URL
	env[EnvRegisterTimeout] = "400ms"
	withEnv(t, env)

	answer, err := phase(t, Ready, readyRequest{Binding: testBinding(t)})
	if err != nil {
		t.Fatalf("Ready: %v", err)
	}
	if _, err := phase(t, Register, registerRequest{Binding: testBinding(t), Session: answer.(attempt.Session)}); err == nil {
		t.Fatal("Register answered a success it never observed")
	}
}

// TestRunKeepsArrivalAndInterruptionApart pins the dispatch answer: an arrived
// payload stays "delivered" when the session dies mid-run, and is never
// reported as not delivered (which is what would invite a replay).
func TestRunKeepsArrivalAndInterruptionApart(t *testing.T) {
	env := adapterEnv(t, probeAwareCarrier)
	started := atomic.Bool{}
	server := controlServer(t, func() map[string]any {
		return map[string]any{
			"attempt": map[string]any{"task_id": "task-1"},
			"task": map[string]any{
				"state": "running", "sent": true, "started": started.Load(), "delivered": started.Load(),
			},
		}
	})
	env[EnvControlURL] = server.URL
	env[EnvDeliveryTimeout] = "5s"
	withEnv(t, env)

	answer, err := phase(t, Ready, readyRequest{Binding: testBinding(t)})
	if err != nil {
		t.Fatalf("Ready: %v", err)
	}
	session := answer.(attempt.Session)
	started.Store(true)

	record, err := resolveSession(mustSettings(t), session)
	if err != nil {
		t.Fatalf("resolve the session record: %v", err)
	}
	if err := stopProcess(record.PID, 5*time.Second); err != nil {
		t.Fatalf("stop the session: %v", err)
	}
	answer, err = phase(t, Run, runRequest{Binding: testBinding(t), Session: session})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	result, ok := answer.(runAnswer)
	if !ok {
		t.Fatalf("Run answered %T, want the dispatch result", answer)
	}
	if !result.Delivered {
		t.Fatal("Run reported an arrived payload as not delivered: that answer invites a replay")
	}
	if result.Error == "" {
		t.Error("Run reported an interrupted dispatch as a clean result")
	}
}

// TestRunNeverUpgradesASentButUnarrivedPayload pins the response-loss path:
// the server committed the delivery, the daemon never started it, and the
// session then exits. The answer is "not delivered", never "delivered".
func TestRunNeverUpgradesASentButUnarrivedPayload(t *testing.T) {
	env := adapterEnv(t, probeAwareCarrier)
	server := controlServer(t, func() map[string]any {
		return map[string]any{
			"attempt": map[string]any{"task_id": "task-1"},
			"task": map[string]any{
				"state": "delivering", "sent": true, "delivered": false, "claims_after_delivery": 2,
			},
		}
	})
	env[EnvControlURL] = server.URL
	env[EnvDeliveryTimeout] = "5s"
	withEnv(t, env)

	answer, err := phase(t, Ready, readyRequest{Binding: testBinding(t)})
	if err != nil {
		t.Fatalf("Ready: %v", err)
	}
	session := answer.(attempt.Session)
	record, err := resolveSession(mustSettings(t), session)
	if err != nil {
		t.Fatalf("resolve the session record: %v", err)
	}
	if err := stopProcess(record.PID, 5*time.Second); err != nil {
		t.Fatalf("stop the session: %v", err)
	}
	answer, err = phase(t, Run, runRequest{Binding: testBinding(t), Session: session})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	result, ok := answer.(runAnswer)
	if !ok {
		t.Fatalf("Run answered %T, want the dispatch result", answer)
	}
	if result.Delivered {
		t.Fatal("Run upgraded a sent-but-unarrived payload to delivered: that answer hides a lost response")
	}
	if result.Error != "not_delivered" {
		t.Errorf("Run error = %q, want not_delivered", result.Error)
	}
}

// TestRunTreatsALostFinalObservationAsUnknown pins the last fabrication path:
// a not-arrived snapshot followed by an unreadable projection is an error, so
// the supervisor isolates the attempt instead of replaying or guessing.
func TestRunTreatsALostFinalObservationAsUnknown(t *testing.T) {
	env := adapterEnv(t, probeAwareCarrier)
	failing := atomic.Bool{}
	observed := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fmj_test" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path == ControlPath+"/environment" {
			_ = json.NewEncoder(w).Encode(map[string]any{"tree_id": "generation-1", "identity": "identity-1", "state": "observed"})
			return
		}
		if failing.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"attempt":     map[string]any{"task_id": "task-1"},
			"environment": map[string]any{"tree_id": "generation-1", "identity": "identity-1", "state": "ok"},
			"task":        map[string]any{"state": "delivering", "sent": true, "delivered": false},
		})
		select {
		case observed <- struct{}{}:
		default:
		}
	}))
	t.Cleanup(server.Close)
	env[EnvControlURL] = server.URL
	env[EnvDeliveryTimeout] = "30s"
	withEnv(t, env)

	answer, err := phase(t, Ready, readyRequest{Binding: testBinding(t)})
	if err != nil {
		t.Fatalf("Ready: %v", err)
	}
	session := answer.(attempt.Session)
	// Ready read the projection once; only Run's own read should count as the
	// successful observation this test waits for.
	select {
	case <-observed:
	default:
	}
	record, err := resolveSession(mustSettings(t), session)
	if err != nil {
		t.Fatalf("resolve the session record: %v", err)
	}

	body, err := json.Marshal(runRequest{Binding: testBinding(t), Session: session})
	if err != nil {
		t.Fatalf("encode the run request: %v", err)
	}
	result := make(chan error, 1)
	go func() {
		_, runErr := Run(context.Background(), body)
		result <- runErr
	}()
	// Wait until Run has actually read "sent, not delivered": only then does
	// a lost final observation risk being mistaken for arrival.
	select {
	case <-observed:
	case <-time.After(10 * time.Second):
		t.Fatal("Run never observed the delivery state")
	}
	failing.Store(true)
	if err := stopProcess(record.PID, 5*time.Second); err != nil {
		t.Fatalf("stop the session: %v", err)
	}
	select {
	case runErr := <-result:
		if runErr == nil {
			t.Fatal("Run upgraded a sent-but-unarrived read to delivered after the final observation failed")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not answer after the final observation failed")
	}
}

// TestRunBoundedWhenTheProjectionKeepsFailing pins the dispatch deadline on
// the path where one successful read is followed by persistent failures and
// the carrier stays alive. The attempt is unknown, so Run must end within the
// window with an error (isolate, no replay) instead of polling forever.
func TestRunBoundedWhenTheProjectionKeepsFailing(t *testing.T) {
	env := adapterEnv(t, probeAwareCarrier)
	var failing atomic.Bool
	observed := make(chan struct{}, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fmj_test" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path == ControlPath+"/environment" {
			_ = json.NewEncoder(w).Encode(map[string]any{"tree_id": "generation-1", "identity": "identity-1", "state": "observed"})
			return
		}
		if failing.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"attempt":     map[string]any{"task_id": "task-1"},
			"environment": map[string]any{"tree_id": "generation-1", "identity": "identity-1", "state": "ok"},
			"task":        map[string]any{"state": "delivering", "sent": true, "delivered": false},
		})
		select {
		case observed <- struct{}{}:
		default:
		}
	}))
	t.Cleanup(server.Close)
	env[EnvControlURL] = server.URL
	env[EnvDeliveryTimeout] = "2s"
	withEnv(t, env)

	answer, err := phase(t, Ready, readyRequest{Binding: testBinding(t)})
	if err != nil {
		t.Fatalf("Ready: %v", err)
	}
	session := answer.(attempt.Session)
	record, err := resolveSession(mustSettings(t), session)
	if err != nil {
		t.Fatalf("resolve the session record: %v", err)
	}
	// Ready read the projection once; the observation this test waits for is
	// Run's own.
	select {
	case <-observed:
	default:
	}

	body, err := json.Marshal(runRequest{Binding: testBinding(t), Session: session})
	if err != nil {
		t.Fatalf("encode the run request: %v", err)
	}
	result := make(chan error, 1)
	go func() {
		_, runErr := Run(context.Background(), body)
		result <- runErr
	}()
	select {
	case <-observed:
	case <-time.After(5 * time.Second):
		t.Fatal("Run never observed the delivery state")
	}
	failing.Store(true)

	select {
	case runErr := <-result:
		if runErr == nil {
			t.Fatal("Run reported a clean result from an unreadable projection")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not enforce the dispatch deadline while the carrier stayed alive")
	}
	if !processAlive(record.PID) {
		t.Fatal("the carrier exited; this case must fail only because the projection became unreadable")
	}
	_ = stopProcess(record.PID, 5*time.Second)
}

func mustSettings(t *testing.T) Settings {
	t.Helper()
	settings, err := LoadSettings()
	if err != nil {
		t.Fatalf("load the adapter settings: %v", err)
	}
	return settings
}

// TestStatusAndSlotTransitionsReadTheSharedLedger pins the ledger seam: the
// status answer is the ledger's, and every transition posts the attempt's own
// claim without inventing ids.
func TestStatusAndSlotTransitionsReadTheSharedLedger(t *testing.T) {
	env := adapterEnv(t, probeAwareCarrier)
	var seen atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fmj_test" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"attempt": map[string]any{"task_id": "task-1"},
				"slot":    map[string]any{"exists": true, "state": "active", "phase": "opening", "claim_id": "c-1"},
			})
			return
		}
		seen.Add(1)
		var body SlotRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if body.ClaimID != "c-1" {
			w.WriteHeader(http.StatusConflict)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"slot": map[string]any{"exists": true, "state": "active", "claim_id": body.ClaimID}})
	}))
	t.Cleanup(server.Close)
	env[EnvControlURL] = server.URL
	withEnv(t, env)

	answer, err := phase(t, Status, slotRequest{Binding: testBinding(t)})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	status, ok := answer.(attempt.SlotStatus)
	if !ok {
		t.Fatalf("Status answered %T, want the ledger's slot", answer)
	}
	if !status.Exists || status.Phase != "opening" || status.ClaimID != "c-1" {
		t.Errorf("status = %+v, want the ledger's own record", status)
	}
	for _, tc := range []struct {
		name    string
		fn      PhaseFunc
		request any
	}{
		{name: "begin", fn: Begin, request: slotRequest{Binding: testBinding(t), ClaimID: "c-1"}},
		{name: "started", fn: MarkStarted, request: slotRequest{Binding: testBinding(t), ClaimID: "c-1"}},
		{name: "release", fn: Release, request: releaseRequest{Binding: testBinding(t), ClaimID: "c-1", Confirmed: true}},
	} {
		if _, err := phase(t, tc.fn, tc.request); err != nil {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
	if seen.Load() != 3 {
		t.Errorf("ledger transitions = %d, want one per phase", seen.Load())
	}
}

// TestPrepareFetchesOnlyTheFrozenArtifact pins the CLI adapter: the artifact
// comes from the frozen proxy address and its digest is checked before the
// path is answered.
func TestPrepareFetchesOnlyTheFrozenArtifact(t *testing.T) {
	body := []byte("#!/bin/sh\necho multica\n")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fmj_test" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	env := adapterEnv(t, probeAwareCarrier)
	env[EnvProxyURL] = server.URL
	withEnv(t, env)

	cli := attempt.CLI{
		Kind: attempt.KindMultica, Version: "0.11.1", Platform: "linux/amd64",
		Digest: digestOfBody(body), DownloadURL: server.URL + "/cli/multica/0.11.1/linux/amd64",
	}
	answer, err := phase(t, Prepare, cli)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	artifact, ok := answer.(attempt.Artifact)
	if !ok {
		t.Fatalf("Prepare answered %T, want the verified artifact", answer)
	}
	if artifact.Digest != cli.Digest || artifact.Path == "" {
		t.Errorf("artifact = %+v, want the frozen digest and a local path", artifact)
	}

	// A digest that does not match the served bytes is refused, not answered
	// with the local path.
	cli.Digest = "sha256:" + strings.Repeat("0", 64)
	if _, err := phase(t, Prepare, cli); err == nil {
		t.Fatal("Prepare accepted an artifact whose bytes do not match the frozen digest")
	}
}

// TestPrepareSharesOneAttemptBudgetAcrossHelpers pins the attempt-wide budget:
// every prepare phase is a separate helper process, so the spent bytes must
// survive the process boundary. Two artifacts that each fit under 100 bytes
// but total 120 must fail on the second one, while a verified cache hit
// consumes nothing.
func TestPrepareSharesOneAttemptBudgetAcrossHelpers(t *testing.T) {
	first := []byte("#!/bin/sh\n" + strings.Repeat("a", 50))
	second := []byte("#!/bin/sh\n" + strings.Repeat("b", 50))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/multica":
			_, _ = w.Write(first)
		case "/agent":
			_, _ = w.Write(second)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	env := adapterEnv(t, probeAwareCarrier)
	env[EnvProxyURL] = server.URL
	env[EnvCLIBudget] = "100"
	withEnv(t, env)

	multica := attempt.CLI{
		Kind: attempt.KindMultica, Version: "0.11.1", Platform: "linux/amd64",
		Digest: digestOfBody(first), DownloadURL: server.URL + "/multica",
	}
	if _, err := phase(t, Prepare, multica); err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	// The same artifact is a cache hit: no bytes are spent.
	if _, err := phase(t, Prepare, multica); err != nil {
		t.Fatalf("cached prepare: %v", err)
	}
	agent := attempt.CLI{
		Kind: attempt.KindAgent, Backend: "omp", Version: "19.0.1", Platform: "linux/amd64",
		Digest: digestOfBody(second), DownloadURL: server.URL + "/agent",
	}
	if _, err := phase(t, Prepare, agent); err == nil {
		t.Fatal("Prepare accepted a second artifact after the attempt's shared budget was spent")
	}
}

// TestPrepareRefusesAfterAnExactExhaustion pins the boundary the shared budget
// missed: an artifact that spends the budget exactly leaves zero bytes, and
// zero must fail the next cache miss instead of falling back to a fresh
// default. A verified cache hit of the already-fetched artifact still works.
func TestPrepareRefusesAfterAnExactExhaustion(t *testing.T) {
	first := []byte("#!/bin/sh\n" + strings.Repeat("a", 90))
	if len(first) != 100 {
		t.Fatalf("the fixture is %d bytes, want exactly 100", len(first))
	}
	second := []byte("#!")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/multica":
			_, _ = w.Write(first)
		case "/agent":
			_, _ = w.Write(second)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	env := adapterEnv(t, probeAwareCarrier)
	env[EnvProxyURL] = server.URL
	env[EnvCLIBudget] = "100"
	withEnv(t, env)

	multica := attempt.CLI{
		Kind: attempt.KindMultica, Version: "0.11.1", Platform: "linux/amd64",
		Digest: digestOfBody(first), DownloadURL: server.URL + "/multica",
	}
	if _, err := phase(t, Prepare, multica); err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	agent := attempt.CLI{
		Kind: attempt.KindAgent, Backend: "omp", Version: "19.0.1", Platform: "linux/amd64",
		Digest: digestOfBody(second), DownloadURL: server.URL + "/agent",
	}
	if _, err := phase(t, Prepare, agent); err == nil {
		t.Fatal("Prepare refetched a new artifact after the budget was exhausted exactly")
	}
	// The cached bytes are still the frozen artifact, so a cache hit is allowed.
	if _, err := phase(t, Prepare, multica); err != nil {
		t.Fatalf("a verified cache hit was refused after exhaustion: %v", err)
	}
}

// TestRunFinalObservationHonorsTheDispatchDeadline pins the last read that can
// outlive the window: after a not-arrived observation the carrier exits and
// the Job face accepts the final projection request but never answers it. The
// dispatch deadline — not the HTTP client's separate 30s timeout — must bound
// that read, so Run ends unknown within the configured window and never
// replays the attempt.
func TestRunFinalObservationHonorsTheDispatchDeadline(t *testing.T) {
	env := adapterEnv(t, probeAwareCarrier)
	var requests atomic.Int32
	var hang atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fmj_test" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path == ControlPath+"/environment" {
			_ = json.NewEncoder(w).Encode(map[string]any{"tree_id": "generation-1", "identity": "identity-1", "state": "observed"})
			return
		}
		if hang.Load() {
			// Accept the request and never answer it: only the client's
			// deadline can end this read.
			<-r.Context().Done()
			return
		}
		n := requests.Add(1)
		if n == 1 {
			// The only successful read proves "sent, not arrived"; the next
			// read is the final observation after the carrier exits.
			hang.Store(true)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"attempt":     map[string]any{"task_id": "task-1"},
			"environment": map[string]any{"tree_id": "generation-1", "identity": "identity-1", "state": "ok"},
			"task":        map[string]any{"state": "delivering", "sent": true, "delivered": false},
		})
	}))
	t.Cleanup(server.Close)
	env[EnvControlURL] = server.URL
	env[EnvDeliveryTimeout] = "2s"
	withEnv(t, env)

	answer, err := phase(t, Ready, readyRequest{Binding: testBinding(t)})
	if err != nil {
		t.Fatalf("Ready: %v", err)
	}
	session := answer.(attempt.Session)
	record, err := resolveSession(mustSettings(t), session)
	if err != nil {
		t.Fatalf("resolve the session record: %v", err)
	}
	if err := stopProcess(record.PID, 5*time.Second); err != nil {
		t.Fatalf("stop the session: %v", err)
	}

	body, err := json.Marshal(runRequest{Binding: testBinding(t), Session: session})
	if err != nil {
		t.Fatalf("encode the run request: %v", err)
	}
	start := time.Now()
	result := make(chan error, 1)
	go func() {
		_, runErr := Run(context.Background(), body)
		result <- runErr
	}()
	select {
	case runErr := <-result:
		if runErr == nil {
			t.Fatal("Run reported a clean result from an unanswerable final observation")
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("Run took %s; the final observation did not inherit the 2s dispatch window", elapsed)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("Run let the final observation outlive the dispatch window")
	}
}

// TestSocketBudgetKeepsBothRelayPathsInsideSunPath pins the kernel limit that
// a session start depends on: bind(2) accepts at most 107 bytes of AF_UNIX
// path, and rsbx binds its relay under <cache>/session-<random>/broker. The
// value judged here is read back out of the config the adapter hands rsbx, so
// a render that deepens the cache is caught even though the render and the
// check run in the same process.
func TestSocketBudgetKeepsBothRelayPathsInsideSunPath(t *testing.T) {
	// The reference render's default root, with a UUID-shaped task id: the
	// shape that produced a 109-byte session path and a 115-byte probe path.
	attemptID := "6f1e2b40-5c7a-4d2e-9b3a-1c8d7e6f5a4b-1"
	settings := Settings{SessionDir: "/run/foreman/state/session"}
	binding := attempt.Binding{AttemptID: attemptID}
	if dir := settings.attemptDir(attemptID); len(filepath.Base(dir)) > maxAttemptDirName {
		t.Fatalf("attempt directory name %q is %d bytes, over the %d-byte bound",
			filepath.Base(dir), len(filepath.Base(dir)), maxAttemptDirName)
	}
	for name, config := range map[string]string{
		"session": renderSessionConfig(binding, settings, sessionTarget{}),
		"probe":   renderProbeConfig(settings, binding, sessionTarget{}),
	} {
		cache, err := renderedCacheDir(config)
		if err != nil {
			t.Fatalf("%s render names no cache: %v", name, err)
		}
		if err := checkSocketBudget(cache); err != nil {
			t.Errorf("%s relay: %v", name, err)
		}
		if path := brokerSocketPath(cache); len(path) > maxUnixSocketPath {
			t.Errorf("%s relay socket %q is %d bytes, over %d", name, path, len(path), maxUnixSocketPath)
		}
	}
}

// TestSocketBudgetRefusesAnOverlongRoot is the counterexample the budget
// exists for: the same attempt under a root long enough to overflow must be
// refused, not started and left to fail at bind.
func TestSocketBudgetRefusesAnOverlongRoot(t *testing.T) {
	attemptID := "6f1e2b40-5c7a-4d2e-9b3a-1c8d7e6f5a4b-1"
	settings := Settings{SessionDir: "/run/foreman/state/session/" + strings.Repeat("x", 60)}
	binding := attempt.Binding{AttemptID: attemptID}
	for name, config := range map[string]string{
		"session": renderSessionConfig(binding, settings, sessionTarget{}),
		"probe":   renderProbeConfig(settings, binding, sessionTarget{}),
	} {
		if err := checkRenderedSocketBudget(config); err == nil {
			cache, _ := renderedCacheDir(config)
			t.Fatalf("%s relay under the over-long root was accepted: %q", name, brokerSocketPath(cache))
		}
	}
}

// TestSocketBudgetJudgesTheRenderedCacheNotARecomputedOne is the regression the
// two earlier rounds could not see: the render is the only source of the path
// rsbx binds, so deepening it in the config must fail even when every helper
// still computes a short path. Each carrier config is deepened on its own, so
// neither relay can hide behind the other.
func TestSocketBudgetJudgesTheRenderedCacheNotARecomputedOne(t *testing.T) {
	attemptID := "6f1e2b40-5c7a-4d2e-9b3a-1c8d7e6f5a4b-1"
	settings := Settings{SessionDir: "/run/foreman/state/session"}
	binding := attempt.Binding{AttemptID: attemptID}
	for name, render := range map[string]func() string{
		"session": func() string { return renderSessionConfig(binding, settings, sessionTarget{}) },
		"probe":   func() string { return renderProbeConfig(settings, binding, sessionTarget{}) },
	} {
		healthy := render()
		if err := checkRenderedSocketBudget(healthy); err != nil {
			t.Fatalf("%s render was refused before the path was deepened: %v", name, err)
		}
		// Deepen exactly the cache the render wrote, leaving the helpers that
		// compute it untouched. This is the shape of the regression: the
		// config path overflows while sessionCacheDir/probeCacheDir still do
		// not.
		cache, err := renderedCacheDir(healthy)
		if err != nil {
			t.Fatalf("%s render names no cache: %v", name, err)
		}
		deep := cache
		for len(brokerSocketPath(deep)) <= maxUnixSocketPath {
			deep += "/deep"
		}
		deepened := strings.Replace(healthy, strconv.Quote(cache), strconv.Quote(deep), 1)
		if deepened == healthy {
			t.Fatalf("%s render did not carry the cache %q verbatim", name, cache)
		}
		if got, _ := renderedCacheDir(deepened); got != deep {
			t.Fatalf("%s cache was not deepened: %q", name, got)
		}
		if err := checkRenderedSocketBudget(deepened); err == nil {
			t.Fatalf("%s relay accepted a deepened cache path %q", name, deepened)
		}
	}
}

// TestAttemptDirStaysDistinctAndStable: shortening must not collide two
// attempts or move an attempt's directory between the adapter processes that
// recompute it.
func TestAttemptDirStaysDistinctAndStable(t *testing.T) {
	settings := Settings{SessionDir: "/run/foreman/state/session"}
	first := "6f1e2b40-5c7a-4d2e-9b3a-1c8d7e6f5a4b-1"
	second := "6f1e2b40-5c7a-4d2e-9b3a-1c8d7e6f5a4b-2"
	a, b := settings.attemptDir(first), settings.attemptDir(second)
	if a == b {
		t.Fatalf("two attempts share the directory %q", a)
	}
	if again := settings.attemptDir(first); again != a {
		t.Fatalf("attempt directory moved between calls: %q then %q", a, again)
	}
}
