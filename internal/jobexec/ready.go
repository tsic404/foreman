package jobexec

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/tsic404/foreman/internal/attempt"
	"github.com/tsic404/foreman/internal/rsbxendpoint"
)

// probeStdoutLimit bounds the ready answer this adapter is willing to read.
// The probe prints one marker and nothing else; anything larger is noise from
// the carrier, not evidence.
const probeStdoutLimit = 4 << 10

// The files one projected environment TLS reference carries: the trust
// anchor, the client certificate and its private key, plus the optional
// expected server name.
const (
	tlsCAFile         = "ca.pem"
	tlsCertFile       = "cert.pem"
	tlsKeyFile        = "key.pem"
	tlsServerNameFile = "server_name"
)

// tlsMaterial is the authentication of one authenticated network session. The
// trust anchor, the client certificate and its private key are all read from
// the deployment's projected material; a channel that authenticates by socket
// permissions or by the deployer's ssh configuration may carry only the trust
// anchor.
type tlsMaterial struct {
	CA          string
	Cert        string
	Key         string
	ServerName  string
	Fingerprint string
}

// resolveTLSMaterial turns the frozen TLS reference into the material the
// carrier authenticates with. An authenticated network channel needs the
// complete set: a trust anchor, a client certificate and its private key. A
// deployment that configures a TLS directory makes the reference mandatory; a
// missing, unreadable or unusable reference refuses the session before
// anything starts. Without such a directory the reference is optional for the
// channels that authenticate through the deployment (a unix socket's
// permissions or the deployer's ssh configuration).
func resolveTLSMaterial(settings Settings, ref, transport string) (tlsMaterial, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		if transport == rsbxendpoint.TransportTLS {
			return tlsMaterial{}, errors.New("the authenticated network transport needs the frozen TLS reference")
		}
		return tlsMaterial{}, nil
	}
	configured := strings.TrimSpace(os.Getenv(EnvTLSDir)) != ""
	resolved := ref
	if !filepath.IsAbs(resolved) {
		if !configured {
			if transport == rsbxendpoint.TransportTLS {
				return tlsMaterial{}, fmt.Errorf("the frozen TLS reference %q cannot resolve: the deployment configures no TLS directory", ref)
			}
			return tlsMaterial{}, nil
		}
		resolved = filepath.Join(settings.TLSDir, resolved)
	}
	resolved = filepath.Clean(resolved)
	if !filepath.IsAbs(resolved) {
		return tlsMaterial{}, fmt.Errorf("the frozen TLS reference %q does not resolve to an absolute path", ref)
	}
	if !filepath.IsAbs(ref) {
		relative, relErr := filepath.Rel(settings.TLSDir, resolved)
		if relErr != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return tlsMaterial{}, fmt.Errorf("the frozen TLS reference %q escapes %s", ref, settings.TLSDir)
		}
	}
	info, statErr := os.Stat(resolved)
	if statErr != nil {
		return tlsMaterial{}, fmt.Errorf("the frozen TLS material %q is not available: %w", ref, statErr)
	}
	if !info.IsDir() {
		if transport == rsbxendpoint.TransportTLS {
			return tlsMaterial{}, fmt.Errorf("the frozen TLS material %q is a single bundle: the authenticated network transport needs the trust anchor, client certificate and private key", ref)
		}
		body, readErr := os.ReadFile(resolved)
		if readErr != nil {
			return tlsMaterial{}, fmt.Errorf("the frozen TLS material %q is not available: %w", ref, readErr)
		}
		if !hasCertificate(body) {
			return tlsMaterial{}, fmt.Errorf("the frozen TLS material %q carries no parseable certificate", ref)
		}
		sum := sha256.Sum256(body)
		return tlsMaterial{CA: resolved, Fingerprint: hex.EncodeToString(sum[:])}, nil
	}
	material := tlsMaterial{
		CA:   filepath.Join(resolved, tlsCAFile),
		Cert: filepath.Join(resolved, tlsCertFile),
		Key:  filepath.Join(resolved, tlsKeyFile),
	}
	caBody, readErr := os.ReadFile(material.CA)
	if readErr != nil {
		return tlsMaterial{}, fmt.Errorf("the frozen TLS material %q carries no trust anchor: %w", ref, readErr)
	}
	if !hasCertificate(caBody) {
		return tlsMaterial{}, fmt.Errorf("the frozen TLS material %q carries no parseable trust anchor", ref)
	}
	if transport != rsbxendpoint.TransportTLS {
		sum := sha256.Sum256(caBody)
		material.Cert, material.Key = "", ""
		material.Fingerprint = hex.EncodeToString(sum[:])
		return material, nil
	}
	certBody, readErr := os.ReadFile(material.Cert)
	if readErr != nil {
		return tlsMaterial{}, fmt.Errorf("the frozen TLS material %q carries no client certificate: %w", ref, readErr)
	}
	keyBody, readErr := os.ReadFile(material.Key)
	if readErr != nil {
		return tlsMaterial{}, fmt.Errorf("the frozen TLS material %q carries no client key: %w", ref, readErr)
	}
	if _, pairErr := tls.X509KeyPair(certBody, keyBody); pairErr != nil {
		return tlsMaterial{}, fmt.Errorf("the frozen TLS material %q is not a usable client certificate and key: %w", ref, pairErr)
	}
	if body, readErr := os.ReadFile(filepath.Join(resolved, tlsServerNameFile)); readErr == nil {
		material.ServerName = strings.TrimSpace(string(body))
	}
	sum := sha256.New()
	sum.Write(caBody)
	sum.Write([]byte{0})
	sum.Write(certBody)
	sum.Write([]byte{0})
	sum.Write([]byte(material.ServerName))
	material.Fingerprint = hex.EncodeToString(sum.Sum(nil))
	return material, nil
}

// hasCertificate accepts a PEM bundle with at least one X.509 certificate. A
// bundle of CAs is the normal shape; a single leaf certificate is accepted
// too, because the carrier's own trust configuration decides how it is used.
func hasCertificate(body []byte) bool {
	for rest := body; ; {
		block, remaining := pem.Decode(rest)
		if block == nil {
			return false
		}
		if block.Type == "CERTIFICATE" {
			if _, err := x509.ParseCertificate(block.Bytes); err == nil {
				return true
			}
		}
		rest = remaining
	}
}

// probeSession runs one bounded command through the carrier before the
// long-lived session starts. The marker proves the carrier executed something
// in the remote view, which is the ready evidence. The frozen tree generation
// and target identity are proven before the probe by the control plane's
// verified fact; the probe names no path of its own, because a registered
// state root is the deployer's host-side identity, not a path the daemon was
// promised to expose. A hang, a missing marker or an unobservable answer is
// not ready.
func probeSession(ctx context.Context, binding attempt.Binding, settings Settings, target sessionTarget) error {
	marker, err := probeMarker()
	if err != nil {
		return err
	}
	dir := settings.attemptDir(binding.AttemptID)
	config := renderProbeConfig(settings, binding, target)
	if err := checkRenderedSocketBudget(config); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create the session directory: %w", err)
	}
	configPath := filepath.Join(dir, "probe.toml")
	if err := writeFileAtomic(configPath, []byte(config), 0o600); err != nil {
		return err
	}
	script := probeScript(marker)
	probeCtx, cancel := context.WithTimeout(ctx, settings.ReadyTimeout)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, settings.RsbxBinary, "-config", configPath, "/bin/sh", "-c", script)
	cmd.Dir = settings.Home
	cmd.Env = carrierEnv(target.TLS.CA)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = 2 * time.Second
	logPath := filepath.Join(dir, sessionLogFile)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open the session log: %w", err)
	}
	defer func() { _ = logFile.Close() }()
	var stdout boundedBuffer
	cmd.Stdout = &stdout
	cmd.Stderr = logFile
	runErr := cmd.Run()
	if probeCtx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("the ready probe did not answer within %s", settings.ReadyTimeout)
	}
	if runErr != nil {
		detail := logTail(logPath)
		if strings.Contains(detail, "daemon unreachable") {
			return fmt.Errorf("%w: %s", ErrDaemonUnreachable, detail)
		}
		return fmt.Errorf("the ready probe failed: %w: %s", runErr, detail)
	}
	if !strings.Contains(stdout.String(), marker) {
		return fmt.Errorf("the ready probe answered no marker: %s", logTail(logPath))
	}
	return nil
}

// probeScript is the one command the probe hands the carrier. The marker is
// the whole check: its arrival proves the remote view executed the command.
// Nothing here opens a host path, so a state root the daemon does not expose
// cannot turn a working session into "no session was created".
func probeScript(marker string) string {
	return fmt.Sprintf("printf '%%s' %s", shellQuote(marker))
}

func probeMarker() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("mint the ready probe marker: %w", err)
	}
	return "foreman-ready-" + hex.EncodeToString(raw[:]), nil
}

// renderProbeConfig is the session configuration with a shell as the entry.
// It keeps the same transport, home and CLI policy as the real session, so the
// probe exercises the channel the session will use.
func renderProbeConfig(settings Settings, binding attempt.Binding, target sessionTarget) string {
	var b strings.Builder
	b.WriteString("# Rendered by rsbx-remote for the ready probe.\n")
	b.WriteString("[daemon]\n")
	writeDaemonTransport(&b, target)
	b.WriteString("\n[local]\n")
	b.WriteString("entry = \"/bin/sh\"\n")
	b.WriteString("allow = [\"/bin/sh\"]\n")
	fmt.Fprintf(&b, "paths = [%q]\n", settings.Home)
	fmt.Fprintf(&b, "read_only_paths = [%q]\n", settings.CLIDir)
	b.WriteString("\n[sandbox]\n")
	dir := settings.attemptDir(binding.AttemptID)
	fmt.Fprintf(&b, "mount = %q\n", filepath.Join(dir, "probe-sandbox"))
	fmt.Fprintf(&b, "cache = %q\n", settings.probeCacheDir(binding.AttemptID))
	b.WriteString("unshare_user = false\n")
	return b.String()
}

// carrierEnv gives the carrier the resolved trust bundle. It inherits the
// process environment because the ssh transport resolves the deployer's own
// configuration from it.
func carrierEnv(tlsPath string) []string {
	env := os.Environ()
	if tlsPath == "" {
		return env
	}
	return append(env, "SSL_CERT_FILE="+tlsPath)
}

// sessionEnv is the real session's carrier environment: the trust bundle plus
// the selected Agent backend's launcher path. The daemon runs as the local
// entry and starts the Agent itself, so without this it searches PATH in the
// Pod, where the downloaded artifact has no such name, and exits with "no
// agent CLI found".
func sessionEnv(binding attempt.Binding, settings Settings, tlsPath string) []string {
	env := carrierEnv(tlsPath)
	if name, value, ok := agentBackendEnv(binding, settings); ok {
		env = append(env, name+"="+value)
	}
	return env
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

type boundedBuffer struct {
	body []byte
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	room := probeStdoutLimit - len(b.body)
	if room > 0 {
		if len(p) < room {
			room = len(p)
		}
		b.body = append(b.body, p[:room]...)
	}
	return len(p), nil
}

func (b *boundedBuffer) String() string { return string(b.body) }
