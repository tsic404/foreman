package jobexec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/tsic404/foreman/internal/attempt"
	"github.com/tsic404/foreman/internal/rsbxendpoint"
)

// sessionRecordFile is the per-attempt session state. It survives the
// short-lived adapter processes: ready writes it, register and run read it to
// judge liveness, and close removes it only after the session process is gone.
const sessionRecordFile = "session.json"

// sessionConfigFile is the rsbx configuration ready renders from the frozen
// binding.
const sessionConfigFile = "rsbx.toml"

// sessionLogFile keeps the session's own output. The adapter never parses it
// as a protocol; it is read only to report why a session did not come up.
const sessionLogFile = "session.log"

// ErrDaemonUnreachable is the carrier's own preflight failure: rsbx reports
// "daemon unreachable" before it mounts anything, so no session was created.
var ErrDaemonUnreachable = errors.New("the frozen endpoint did not answer")

// SessionID names one attempt's remote session. rsbx has no session token of
// its own, so the identity is derived from the frozen binding: the same
// environment, tree generation, endpoint and attempt name the same session
// across Pod rebuilds, which is exactly what a resumed attempt must address.
func SessionID(binding attempt.Binding) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		"foreman-rsbx-session",
		binding.EnvironmentID,
		binding.EnvironmentGen,
		binding.Endpoint,
		binding.AttemptID,
	}, "\x00")))
	return "sbx-" + hex.EncodeToString(sum[:])[:32]
}

// SessionRecord is the local half of a session: the process the adapter
// started and the frozen identity it was opened under.
type SessionRecord struct {
	ID            string `json:"id"`
	AttemptID     string `json:"attempt_id"`
	EnvironmentID string `json:"environment_id"`
	Generation    string `json:"generation"`
	Endpoint      string `json:"endpoint"`
	TLSRef        string `json:"tls_ref,omitempty"`
	// TLSFingerprint is the sha256 of the resolved trust bundle. It records
	// which material the session was actually opened with; an unresolved
	// reference never reaches this record.
	TLSFingerprint string    `json:"tls_fingerprint,omitempty"`
	PID            int       `json:"pid"`
	StartedAt      time.Time `json:"started_at"`
	ConfigPath     string    `json:"config_path"`
	LogPath        string    `json:"log_path"`
}

// Session is the frozen identity this record answers.
func (r SessionRecord) Session() attempt.Session {
	return attempt.Session{
		ID:             r.ID,
		EnvironmentID:  r.EnvironmentID,
		Generation:     r.Generation,
		Endpoint:       r.Endpoint,
		TLSFingerprint: r.TLSFingerprint,
	}
}

// maxAttemptDirName bounds the per-attempt directory name. The broker relays
// its socket through <session root>/<attempt>/<cache>/session-<random>/broker,
// and a unix socket path must fit the kernel's 108-byte sun_path including its
// terminator. An attempt id is a task id plus its attempt number, so a UUID
// task alone would spend 38 bytes of that budget; the directory name is
// shortened deterministically instead of letting the path overflow at bind
// time, when the only symptom is a bare EINVAL.
const maxAttemptDirName = 20

// maxUnixSocketPath is the longest path bind(2) accepts for AF_UNIX: sun_path
// is 108 bytes with the trailing NUL, so 107 bytes of path.
const maxUnixSocketPath = 107

// attemptDir is the attempt's private directory under the session root. The
// attempt id is reduced to a safe name so one attempt cannot address another's
// files. A name longer than maxAttemptDirName keeps a readable prefix and
// carries a digest of the whole id, so the mapping stays deterministic across
// the adapter processes that recompute it and two attempts never share a
// directory.
func (s Settings) attemptDir(attemptID string) string {
	name := sanitize(attemptID)
	if len(name) > maxAttemptDirName {
		sum := sha256.Sum256([]byte(attemptID))
		name = name[:maxAttemptDirName-11] + "-" + hex.EncodeToString(sum[:])[:10]
	}
	return filepath.Join(s.SessionDir, name)
}

// brokerSocketPath is the socket rsbx binds for one attempt's sandbox relay.
// rsbx creates its session directory under the configured cache with a
// "session-" prefix and a decimal random suffix of at most ten digits, then
// places the broker socket at "broker/daemon.sock" inside it.
func brokerSocketPath(cacheDir string) string {
	const sessionDirPrefix = "/session-"
	const randomDigits = 10
	return cacheDir + sessionDirPrefix + strings.Repeat("9", randomDigits) + "/broker/daemon.sock"
}

// checkSocketBudget refuses a sandbox layout whose relay socket could not be
// bound. The kernel answers an over-long path with EINVAL at session start,
// which reads as a generic carrier failure; failing here names the path and
// its length instead.
func checkSocketBudget(cacheDir string) error {
	path := brokerSocketPath(cacheDir)
	if len(path) > maxUnixSocketPath {
		return fmt.Errorf("the sandbox relay socket %q is %d bytes, over the %d-byte limit: shorten the session root or the attempt directory",
			path, len(path), maxUnixSocketPath)
	}
	return nil
}

// probeCacheDir is the cache the ready probe starts its carrier session under.
// It is a sibling of the real session's cache, so it carries the same
// sun_path budget.
func (s Settings) probeCacheDir(attemptID string) string {
	return filepath.Join(s.attemptDir(attemptID), "probe-cache")
}

// renderedCacheDir reads the sandbox cache back out of a rendered carrier
// config. The budget check judges this value, not a path recomputed beside the
// render: rsbx binds its relay under whatever the config names, so a render
// that deepens the path must be what the check sees.
func renderedCacheDir(config string) (string, error) {
	for _, line := range strings.Split(config, "\n") {
		key, value, ok := strings.Cut(line, "=")
		if !ok || strings.TrimSpace(key) != "cache" {
			continue
		}
		cache, err := strconv.Unquote(strings.TrimSpace(value))
		if err != nil {
			return "", fmt.Errorf("the rendered sandbox cache is not a quoted path: %w", err)
		}
		return cache, nil
	}
	return "", errors.New("the rendered carrier config names no sandbox cache")
}

// checkRenderedSocketBudget refuses a rendered carrier config whose relay
// socket could not be bound. Reading the cache out of the config keeps the
// check on the same value rsbx receives.
func checkRenderedSocketBudget(config string) error {
	cache, err := renderedCacheDir(config)
	if err != nil {
		return err
	}
	return checkSocketBudget(cache)
}

// CheckAttemptSocketBudget reports whether both relay sockets an attempt binds
// under this session root fit the kernel's sun_path limit, judged from the
// configs this adapter would hand the carrier. It lets a render-side check
// reach the same verdict for the root a Job names.
func (s Settings) CheckAttemptSocketBudget(attemptID string) error {
	binding := attempt.Binding{AttemptID: attemptID}
	for _, config := range []string{
		renderSessionConfig(binding, s, sessionTarget{}),
		renderProbeConfig(s, binding, sessionTarget{}),
	} {
		if err := checkRenderedSocketBudget(config); err != nil {
			return err
		}
	}
	return nil
}

func sanitize(value string) string {
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "attempt"
	}
	return b.String()
}

func loadRecord(dir string) (SessionRecord, bool, error) {
	body, err := os.ReadFile(filepath.Join(dir, sessionRecordFile))
	if errors.Is(err, os.ErrNotExist) {
		return SessionRecord{}, false, nil
	}
	if err != nil {
		return SessionRecord{}, false, fmt.Errorf("read the session record: %w", err)
	}
	var record SessionRecord
	if err := json.Unmarshal(body, &record); err != nil {
		return SessionRecord{}, false, fmt.Errorf("decode the session record: %w", err)
	}
	return record, true, nil
}

func saveRecord(dir string, record SessionRecord) error {
	body, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode the session record: %w", err)
	}
	return writeFileAtomic(filepath.Join(dir, sessionRecordFile), body, 0o600)
}

func removeRecord(dir string) {
	_ = os.Remove(filepath.Join(dir, sessionRecordFile))
}

// findRecord scans the session root for one session id. Close only receives
// the session identity (the supervisor does not repeat the binding), and a
// Job owns exactly one attempt, so the scan is one directory at most.
func findRecord(root, sessionID string) (SessionRecord, bool, error) {
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return SessionRecord{}, false, nil
	}
	if err != nil {
		return SessionRecord{}, false, fmt.Errorf("read the session root: %w", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		record, ok, err := loadRecord(filepath.Join(root, entry.Name()))
		if err != nil || !ok {
			continue
		}
		if record.ID == sessionID {
			return record, true, nil
		}
	}
	return SessionRecord{}, false, nil
}

func writeFileAtomic(path string, body []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".session-*")
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	name := tmp.Name()
	defer func() { _ = os.Remove(name) }()
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("mark %s: %w", path, err)
	}
	if _, err := tmp.Write(body); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close %s: %w", path, err)
	}
	if err := os.Rename(name, path); err != nil {
		return fmt.Errorf("publish %s: %w", path, err)
	}
	return nil
}

// processAlive reports whether pid still names a running process. An exited
// child that this process has not reaped yet is a zombie, and a zombie holds
// no session: the /proc state is read first so liveness never depends on a
// reaper being scheduled. A process that exists but belongs to another user is
// alive for our purposes: we can neither reap nor reuse it.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	if state, ok := processState(pid); ok {
		return state != 'Z' && state != 'X' && state != 'x'
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// processState reads the state character from /proc/<pid>/stat. The comm field
// may contain spaces and parentheses, so the state is the first field after
// the last ')'.
func processState(pid int) (byte, bool) {
	body, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, false
	}
	end := bytes.LastIndexByte(body, ')')
	if end < 0 || end+2 >= len(body) {
		return 0, false
	}
	return body[end+2], true
}

// stopProcess terminates the session's process group and reports only what it
// observed: an exit, or a process that outlived the grace window.
func stopProcess(pid int, grace time.Duration) error {
	if !processAlive(pid) {
		return nil
	}
	// The session was started with its own session id, so the negative pid
	// names the whole group: the carrier and the entry it started.
	if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		_ = syscall.Kill(pid, syscall.SIGTERM)
	}
	if waitGone(pid, grace) {
		return nil
	}
	_ = syscall.Kill(-pid, syscall.SIGKILL)
	if waitGone(pid, 5*time.Second) {
		return nil
	}
	return fmt.Errorf("session process %d did not exit", pid)
}

func waitGone(pid int, window time.Duration) bool {
	deadline := time.Now().Add(window)
	for {
		if !processAlive(pid) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// sessionTarget is the carrier channel one attempt opens. The endpoint is
// deployer input, so the mapping is explicit and shared with the management
// surface: a unix socket, an authenticated network address, or an explicit
// stdio command. The material of an authenticated channel is resolved before
// anything starts, so a missing or unusable reference is "no session", not a
// handshake surprise.
type sessionTarget struct {
	Transport string
	Socket    string
	Address   string
	Command   string
	TLS       tlsMaterial
}

func resolveSessionTarget(binding attempt.Binding, settings Settings) (sessionTarget, error) {
	parsed, err := rsbxendpoint.Parse(binding.Endpoint)
	if err != nil {
		return sessionTarget{}, err
	}
	material, err := resolveTLSMaterial(settings, binding.TLSRef, parsed.Transport)
	if err != nil {
		return sessionTarget{}, err
	}
	return sessionTarget{
		Transport: parsed.Transport,
		Socket:    parsed.Socket,
		Address:   parsed.Address,
		Command:   parsed.Command,
		TLS:       material,
	}, nil
}

// renderSessionConfig renders the carrier configuration for one attempt. The
// daemon entry and the Agent CLI it launches are the two programs this session
// runs on the client side: the daemon holds the Job's own profile, and the
// Agent artifact exists only in the Pod's CLI directory. Every other program —
// the shell a tool starts, a build tool, a business command — is routed by the
// carrier to the chosen remote tree.
func renderSessionConfig(binding attempt.Binding, settings Settings, target sessionTarget) string {
	entry := settings.Entry()
	var b strings.Builder
	b.WriteString("# Rendered from the frozen attempt binding by rsbx-remote.\n")
	b.WriteString("[daemon]\n")
	writeDaemonTransport(&b, target)
	b.WriteString("\n[local]\n")
	fmt.Fprintf(&b, "entry = %q\n", entry[0])
	fmt.Fprintf(&b, "allow = %s\n", tomlStrings(localPrograms(binding, settings)))
	// The daemon runs as the local entry, but the profile tree it writes —
	// the Job token included — is local, so it dies with the Pod and is never
	// left on the deployer's tree. The downloaded CLIs are readable from both
	// sides and never writable.
	fmt.Fprintf(&b, "paths = [%q]\n", settings.Home)
	fmt.Fprintf(&b, "read_only_paths = [%q]\n", settings.CLIDir)
	b.WriteString("\n[sandbox]\n")
	dir := settings.attemptDir(binding.AttemptID)
	fmt.Fprintf(&b, "mount = %q\n", filepath.Join(dir, "sandbox"))
	fmt.Fprintf(&b, "cache = %q\n", settings.sessionCacheDir(binding.AttemptID))
	b.WriteString("unshare_user = false\n")
	return b.String()
}

// localPrograms is the exact set of programs this session runs on the client
// side: the daemon entry, plus the Agent artifact the daemon starts when the
// binding selects one. The carrier compiles each entry into a startup
// snapshot, so these are files, never a directory that would carry its
// neighbours in with it. A path named here must exist: the snapshot is taken
// at session start.
func localPrograms(binding attempt.Binding, settings Settings) []string {
	entry := settings.Entry()
	programs := []string{entry[0]}
	if _, ok := binding.Agent(); ok {
		if path := settings.AgentPath(); path != entry[0] {
			programs = append(programs, path)
		}
	}
	return programs
}

// agentBackendEnv names the variable the frozen Multica daemon reads to find
// the selected Agent backend. The artifact is downloaded to a Pod-local path
// and is not on the daemon's PATH, so the path is named explicitly.
func agentBackendEnv(binding attempt.Binding, settings Settings) (string, string, bool) {
	agent, ok := binding.Agent()
	if !ok || strings.TrimSpace(agent.Backend) == "" {
		return "", "", false
	}
	return agentLauncherVar(agent.Backend), settings.AgentPath(), true
}

// agentLauncherVars are the backends whose launcher variable does not follow
// MULTICA_<BACKEND>_PATH. The names come from the frozen daemon itself: every
// other backend reads its derived spelling, and these four read another.
var agentLauncherVars = map[string]string{
	"cursor-agent": "MULTICA_CURSOR_PATH",
	"kiro-cli":     "MULTICA_KIRO_PATH",
	"agy":          "MULTICA_ANTIGRAVITY_PATH",
	"qodercli":     "MULTICA_QODER_PATH",
}

// agentLauncherVar is the environment variable the frozen daemon reads for one
// backend: its known exception, or MULTICA_<BACKEND>_PATH with punctuation
// folded to underscores.
func agentLauncherVar(backend string) string {
	name := strings.ToLower(strings.TrimSpace(backend))
	if known, ok := agentLauncherVars[name]; ok {
		return known
	}
	var b strings.Builder
	b.WriteString("MULTICA_")
	for _, r := range strings.ToUpper(name) {
		switch {
		case r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	b.WriteString("_PATH")
	return b.String()
}

// tomlStrings renders a TOML array of quoted strings.
func tomlStrings(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, strconv.Quote(value))
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

// sessionCacheDir is the directory rsbx allocates its session under for the
// real session.
func (s Settings) sessionCacheDir(attemptID string) string {
	return filepath.Join(s.attemptDir(attemptID), "cache")
}

// writeDaemonTransport renders the channel and its authentication. The
// carrier authenticates a network session with mutual TLS: a trust anchor, the
// client certificate and its private key are all named, never inferred from
// the process environment.
func writeDaemonTransport(b *strings.Builder, target sessionTarget) {
	fmt.Fprintf(b, "transport = %q\n", target.Transport)
	switch target.Transport {
	case rsbxendpoint.TransportUnix:
		fmt.Fprintf(b, "socket = %q\n", target.Socket)
	case rsbxendpoint.TransportSSH:
		fmt.Fprintf(b, "command = %q\n", target.Command)
	case rsbxendpoint.TransportTLS:
		fmt.Fprintf(b, "address = %q\n", target.Address)
		fmt.Fprintf(b, "tls_ca = %q\n", target.TLS.CA)
		fmt.Fprintf(b, "tls_cert = %q\n", target.TLS.Cert)
		fmt.Fprintf(b, "tls_key = %q\n", target.TLS.Key)
		if target.TLS.ServerName != "" {
			fmt.Fprintf(b, "server_name = %q\n", target.TLS.ServerName)
		}
	}
}

// prepareProfile materialises the per-Job daemon profile from the credential
// volume. The profile state lives under the Job's own HOME, so it dies with
// the Pod and is never reused across tasks.
func (s Settings) prepareProfile() error {
	if strings.TrimSpace(s.DaemonProfile) == "" {
		return errors.New("the daemon profile is empty")
	}
	profile := s.ProfileDir()
	if err := os.MkdirAll(profile, 0o700); err != nil {
		return fmt.Errorf("create the daemon profile: %w", err)
	}
	body, err := os.ReadFile(s.tokenFile())
	if err != nil {
		return fmt.Errorf("read the Job credential for the daemon profile: %w", err)
	}
	var credential map[string]any
	if err := json.Unmarshal(body, &credential); err != nil {
		return fmt.Errorf("decode the Job credential for the daemon profile: %w", err)
	}
	return writeFileAtomic(filepath.Join(profile, "config.json"), body, 0o400)
}

// tokenFile is where the credential document was read from.
func (s Settings) tokenFile() string {
	if value := strings.TrimSpace(os.Getenv(EnvTokenFile)); value != "" {
		return value
	}
	return defaultTokenFile
}

// startSession launches the carrier detached from this short-lived adapter.
// The returned record is the only handle later phases have; started reports
// whether the carrier process exists, which is what separates "provably no
// session" from "unknown".
func startSession(binding attempt.Binding, settings Settings, target sessionTarget, observedGeneration string) (SessionRecord, bool, error) {
	dir := settings.attemptDir(binding.AttemptID)
	config := renderSessionConfig(binding, settings, target)
	if err := checkRenderedSocketBudget(config); err != nil {
		return SessionRecord{}, false, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return SessionRecord{}, false, fmt.Errorf("create the session directory: %w", err)
	}
	if err := settings.prepareProfile(); err != nil {
		return SessionRecord{}, false, err
	}
	configPath := filepath.Join(dir, sessionConfigFile)
	if err := writeFileAtomic(configPath, []byte(config), 0o600); err != nil {
		return SessionRecord{}, false, err
	}
	logPath := filepath.Join(dir, sessionLogFile)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return SessionRecord{}, false, fmt.Errorf("open the session log: %w", err)
	}
	defer func() { _ = logFile.Close() }()
	entry := settings.Entry()
	args := append([]string{"-config", configPath}, entry...)
	cmd := exec.Command(settings.RsbxBinary, args...)
	cmd.Dir = settings.Home
	cmd.Env = sessionEnv(binding, settings, target.TLS.CA)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return SessionRecord{}, false, fmt.Errorf("start the rsbx session: %w", err)
	}
	record := SessionRecord{
		ID:            SessionID(binding),
		AttemptID:     binding.AttemptID,
		EnvironmentID: binding.EnvironmentID,
		// The generation is the observation compared with the frozen binding,
		// never the binding echoed back as if it were evidence.
		Generation:     observedGeneration,
		Endpoint:       binding.Endpoint,
		TLSRef:         binding.TLSRef,
		TLSFingerprint: target.TLS.Fingerprint,
		PID:            cmd.Process.Pid,
		StartedAt:      time.Now().UTC(),
		ConfigPath:     configPath,
		LogPath:        logPath,
	}
	// The adapter reaps its own child, so a carrier that dies during start-up
	// is an observable exit rather than a zombie the liveness probe would keep
	// reporting as running. A session that outlives this short-lived adapter is
	// reparented; later phases probe its pid from their own process.
	go func() { _ = cmd.Wait() }()
	if err := saveRecord(dir, record); err != nil {
		_ = stopProcess(record.PID, 5*time.Second)
		return SessionRecord{}, true, err
	}
	return record, true, nil
}

// waitSession proves the session survived its own start-up. The carrier exits
// before an entry starts when the daemon does not answer; a process that is
// still alive past the settle window passed that preflight and started the
// entry this attempt addresses.
func waitSession(ctx context.Context, record SessionRecord, settle time.Duration) error {
	deadline := time.Now().Add(settle)
	for {
		if !processAlive(record.PID) {
			return sessionFailure(record)
		}
		if time.Now().After(deadline) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func sessionFailure(record SessionRecord) error {
	detail := logTail(record.LogPath)
	if strings.Contains(detail, "daemon unreachable") {
		return fmt.Errorf("%w: %s", ErrDaemonUnreachable, detail)
	}
	return fmt.Errorf("the rsbx session exited during start-up: %s", detail)
}

func logTail(path string) string {
	body, err := os.ReadFile(path)
	if err != nil {
		return "the session log is unreadable"
	}
	const limit = 800
	text := strings.TrimSpace(string(body))
	if len(text) > limit {
		text = text[len(text)-limit:]
	}
	if text == "" {
		return "the session log is empty"
	}
	return text
}

// resolveSession reads the record a later phase addresses. A missing record is
// an unknown remote state, never "no session": only the phase that refuses to
// start anything may claim that.
func resolveSession(settings Settings, session attempt.Session) (SessionRecord, error) {
	if strings.TrimSpace(session.ID) == "" {
		return SessionRecord{}, errors.New("the request names no session")
	}
	record, ok, err := findRecord(settings.SessionDir, session.ID)
	if err != nil {
		return SessionRecord{}, err
	}
	if !ok {
		return SessionRecord{}, fmt.Errorf("no local record for session %s: the remote state is unknown", session.ID)
	}
	if session.EnvironmentID != "" && record.EnvironmentID != session.EnvironmentID {
		return SessionRecord{}, fmt.Errorf("session %s belongs to environment %q, not %q", session.ID, record.EnvironmentID, session.EnvironmentID)
	}
	return record, nil
}

// ensureExecutable refuses a session entry the Pod cannot start. This happens
// before the carrier runs, so it provably creates no session.
func ensureExecutable(path string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("the session entry %q is not an absolute path", path)
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("the session entry %q is not available: %w", path, err)
	}
	if info.IsDir() || info.Mode().Perm()&0o111 == 0 {
		return fmt.Errorf("the session entry %q is not executable", path)
	}
	return nil
}
