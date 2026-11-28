package discovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

// maxAdapterOutput bounds one adapter's stdout.
const maxAdapterOutput = 1 << 20

// ompVersionPattern is the exact version shape the omp CLI reports.
var ompVersionPattern = regexp.MustCompile(`^[0-9]+(?:\.[0-9]+)*(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$`)

// OMPAdapter runs the omp CLI's model listing command in a private directory.
// It is trusted only after Probe sees the flags the parser relies on.
type OMPAdapter struct {
	Binary string
}

// NewOMPAdapter returns an adapter for the omp CLI. An empty binary means the
// deployment installs no global probe binary: the adapter is then trusted from
// a verified published artifact instead, which is the payload the Job runs.
func NewOMPAdapter(binary string) *OMPAdapter {
	return &OMPAdapter{Binary: strings.TrimSpace(binary)}
}

// Name implements Adapter.
func (a *OMPAdapter) Name() string { return "omp" }

// Kinds implements Adapter.
func (a *OMPAdapter) Kinds() []Kind { return []Kind{KindModels} }

// Probe verifies the help surface the parser depends on.
func (a *OMPAdapter) Probe(ctx context.Context) error {
	if a.Binary == "" {
		return errors.New("no global omp probe binary is configured; trust is established from a verified published artifact")
	}
	if _, err := probeOMPCommandSurface(ctx, a.Binary); err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "foreman-omp-probe-")
	if err != nil {
		return errors.New("omp probe directory is unavailable")
	}
	defer func() { _ = os.RemoveAll(dir) }()
	probe := exec.CommandContext(ctx, a.Binary, "models", "ls", "--json", "--no-extensions")
	probe.Dir = dir
	probe.Env = []string{"HOME=" + dir, "TMPDIR=" + dir, "PATH=" + os.Getenv("PATH")}
	raw, err := probe.Output()
	if err != nil {
		return fmt.Errorf("omp model listing sample failed: %w", err)
	}
	if _, _, err := parseModelList(raw); err != nil {
		return fmt.Errorf("omp model listing sample is not the verified schema: %w", err)
	}
	return nil
}

// ProbeBound verifies the command surface of one exact published payload and
// returns the version that payload reports. It deliberately does not run a
// model sample: the sample needs credentials, while registration only needs
// the binary's fixed commands to exist.
func (a *OMPAdapter) ProbeBound(ctx context.Context, binaryPath string) (string, error) {
	if strings.TrimSpace(binaryPath) == "" {
		return "", errors.New("verified omp binary is not available")
	}
	return probeOMPCommandSurface(ctx, binaryPath)
}

func probeOMPCommandSurface(ctx context.Context, binary string) (string, error) {
	dir, err := os.MkdirTemp("", "foreman-omp-surface-")
	if err != nil {
		return "", errors.New("omp command surface directory is unavailable")
	}
	defer func() { _ = os.RemoveAll(dir) }()
	env := []string{"HOME=" + dir, "TMPDIR=" + dir, "PATH=" + os.Getenv("PATH")}
	version := exec.CommandContext(ctx, binary, "--version")
	version.Dir = dir
	version.Env = env
	output, err := version.Output()
	if err != nil {
		return "", fmt.Errorf("omp version is not readable: %w", err)
	}
	actual, err := parseOMPVersion(output)
	if err != nil {
		return "", err
	}
	command := exec.CommandContext(ctx, binary, "models", "ls", "--help")
	command.Dir = dir
	command.Env = env
	output, err = command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("omp model listing command is not available: %w", err)
	}
	help := string(output)
	if !strings.Contains(help, "--json") || !strings.Contains(help, "--no-extensions") {
		return "", errors.New("omp help does not declare the JSON listing flags")
	}
	return actual, nil
}

// parseOMPVersion accepts the CLI's "omp/<version>" line and the older
// "omp <version>" form, and rejects output that does not carry an exact
// version.
func parseOMPVersion(output []byte) (string, error) {
	line := strings.TrimSpace(string(output))
	if line == "" {
		return "", errors.New("omp version output is empty")
	}
	fields := strings.Fields(line)
	token := ""
	switch {
	case strings.HasPrefix(strings.ToLower(fields[0]), "omp/"):
		token = fields[0][len("omp/"):]
	case len(fields) > 1 && strings.EqualFold(fields[0], "omp"):
		token = fields[1]
	default:
		return "", fmt.Errorf("omp version output %q is not in the verified format", line)
	}
	if !ompVersionPattern.MatchString(token) {
		return "", fmt.Errorf("omp version output %q does not carry an exact version", line)
	}
	return token, nil
}

// RequiresBoundInputs makes the executor use the request's verified CLI and
// configuration instead of a global binary.
func (a *OMPAdapter) RequiresBoundInputs() bool { return true }

// Enumerate runs the fixed command and normalizes its JSON.
func (a *OMPAdapter) Enumerate(ctx context.Context, call Call) (Outcome, error) {
	if call.Kind != KindModels {
		return Outcome{Class: ClassUnsupported, Detail: "omp adapter supports models only"}, nil
	}
	binary := call.BinaryPath
	if binary == "" {
		return Outcome{Class: ClassUnsupported, Detail: "verified omp CLI artifact is not available"}, nil
	}
	args := []string{"models", "ls", "--json", "--no-extensions"}
	if call.ConfigPath != "" {
		args = append(args, "--config")
		args = append(args, call.ConfigPath)
	}
	command := exec.CommandContext(ctx, binary, args...)
	command.Dir = call.Dir
	command.Env = []string{
		"HOME=" + call.Dir,
		"TMPDIR=" + call.Dir,
		"PATH=" + os.Getenv("PATH"),
	}
	var stdout bytes.Buffer
	command.Stdout = &limitedWriter{writer: &stdout, remaining: maxAdapterOutput}
	command.Stderr = io.Discard
	if err := command.Run(); err != nil {
		return Outcome{}, fmt.Errorf("omp model listing exited unsuccessfully: %w", err)
	}
	raw := bytes.TrimSpace(stdout.Bytes())
	if len(raw) == 0 {
		return Outcome{}, errors.New("omp model listing returned no JSON")
	}
	models, provider, err := parseModelList(raw)
	if err != nil {
		return Outcome{}, err
	}
	if len(models) == 0 {
		return Outcome{Class: ClassEmpty, Models: []Model{}, Provider: provider}, nil
	}
	return Outcome{Class: ClassNonEmpty, Models: models, Provider: provider}, nil
}

type modelEntry struct {
	ID       string `json:"id"`
	Provider string `json:"provider"`
	Version  string `json:"version"`
}

func parseModelList(raw []byte) ([]Model, string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || (trimmed[0] != '[' && trimmed[0] != '{') {
		return nil, "", errors.New("omp model listing is not a JSON array or object")
	}
	var list []modelEntry
	if trimmed[0] == '[' {
		if err := json.Unmarshal(trimmed, &list); err != nil {
			return nil, "", errors.New("omp model listing is not the expected JSON")
		}
		models, err := normalizeModels(list, "")
		return models, "", err
	}
	var document struct {
		Provider string        `json:"provider"`
		Models   *[]modelEntry `json:"models"`
	}
	if err := json.Unmarshal(trimmed, &document); err != nil || document.Models == nil {
		return nil, "", errors.New("omp model listing is not the expected JSON")
	}
	models, err := normalizeModels(*document.Models, document.Provider)
	return models, document.Provider, err
}

func normalizeModels(entries []modelEntry, fallbackProvider string) ([]Model, error) {
	out := make([]Model, 0, len(entries))
	for _, entry := range entries {
		id := strings.TrimSpace(entry.ID)
		if id == "" {
			return nil, errors.New("omp model record has no model id")
		}
		provider := strings.TrimSpace(entry.Provider)
		if provider == "" {
			provider = strings.TrimSpace(fallbackProvider)
		}
		if provider == "" {
			return nil, errors.New("omp model record has no provider")
		}
		out = append(out, Model{ID: id, Provider: provider, Version: strings.TrimSpace(entry.Version)})
	}
	return out, nil
}

// UnsupportedAdapter is a backend whose command surface is not verified. It
// answers unsupported, never an empty model list.
type UnsupportedAdapter struct {
	AdapterName  string
	AdapterKinds []Kind
	Reason       string
}

// NewUnsupportedAdapter returns an adapter that refuses every request.
func NewUnsupportedAdapter(name, reason string) *UnsupportedAdapter {
	return &UnsupportedAdapter{AdapterName: name, AdapterKinds: []Kind{KindModels, KindCapabilities}, Reason: reason}
}

// Name implements Adapter.
func (a *UnsupportedAdapter) Name() string { return a.AdapterName }

// Kinds implements Adapter.
func (a *UnsupportedAdapter) Kinds() []Kind { return a.AdapterKinds }

// Enumerate implements Adapter.
func (a *UnsupportedAdapter) Enumerate(context.Context, Call) (Outcome, error) {
	return Outcome{Class: ClassUnsupported, Detail: a.Reason}, nil
}

// limitedWriter fails a command's output once the bound is exceeded, so a
// misbehaving adapter cannot exhaust the control plane's memory.
type limitedWriter struct {
	writer    io.Writer
	remaining int
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if len(p) > w.remaining {
		return 0, errors.New("adapter output exceeds the bound")
	}
	w.remaining -= len(p)
	return w.writer.Write(p)
}
