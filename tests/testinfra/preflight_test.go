package testinfra

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// preflightEntries is each entry script's dependency contract, stated here
// instead of read back from the scripts: a test derived from the script text
// would follow a deleted `need` line rather than fail on it.
var preflightEntries = []struct {
	script   string
	requires []string
}{
	{"deploy.sh", []string{"kubectl", "sed", "jq"}},
	{"recycle.sh", []string{"kubectl"}},
	{"reset.sh", []string{"kubectl", "curl", "jq"}},
	{"selfcheck.sh", []string{"kubectl", "curl", "jq", "multica"}},
	{"wire.sh", []string{"kubectl", "curl", "jq"}},
	{"bind-agent.sh", []string{"curl", "jq"}},
}

// recordingStub stands in for one external command and records that it ran.
// It always succeeds: a script that slipped past its own check has to run on
// and leave a trail, and the call log is the only verdict.
const recordingStub = `#!/bin/sh
printf '%s\n' "${0##*/} $*" >>"$CALL_LOG"
exit 0
`

// TestEntriesReportAMissingCommandBeforeRunningAnything pins all three faces of
// the preflight contract at once: exit code 1, the shared die() message naming
// the command, and an empty call log. The log is what separates a check that
// runs first from one moved below the first kubectl call — the latter still
// exits 1 with the same message after the cluster has already been touched.
// The design repo leaves the test infrastructure's implementation out of scope
// (01-scope, 明确不做：测试基础设施的实现体); this is that implementation's own
// regression pin.
func TestEntriesReportAMissingCommandBeforeRunningAnything(t *testing.T) {
	for _, entry := range preflightEntries {
		for _, missing := range entry.requires {
			t.Run(entry.script+"/missing-"+missing, func(t *testing.T) {
				run := runEntry(t, entry.script, entry.requires, missing)

				if run.exitCode != 1 {
					t.Errorf("exit code = %d, want 1\n--- stderr ---\n%s", run.exitCode, run.stderr)
				}
				want := "test-multica: required command not found: " + missing
				if !strings.Contains(run.stderr, want) {
					t.Errorf("stderr does not carry %q\n--- stderr ---\n%s", want, run.stderr)
				}
				if run.calls != "" {
					t.Errorf("commands ran before the preflight check reported:\n%s", run.calls)
				}
			})
		}
	}
}

type entryRun struct {
	exitCode int
	stderr   string
	calls    string
}

// runEntry runs one entry script with `missing` absent from PATH and every
// other required command replaced by a recording stub, so any use of one shows
// up in the call log.
func runEntry(t *testing.T, script string, requires []string, missing string) entryRun {
	t.Helper()
	shPath, err := exec.LookPath("sh")
	if err != nil {
		t.Skipf("host has no sh: %v", err)
	}
	scriptPath, err := filepath.Abs(filepath.Join(libDir, script))
	if err != nil {
		t.Fatalf("resolve %s: %v", script, err)
	}
	dir := t.TempDir()
	binDir := filepath.Join(dir, "bin")
	if err := os.Mkdir(binDir, 0o755); err != nil {
		t.Fatalf("create stub dir: %v", err)
	}
	for _, command := range requires {
		if command == missing {
			continue
		}
		writeExecutable(t, filepath.Join(binDir, command), recordingStub)
	}
	// lib.sh resolves its own directory before the first preflight check, so the
	// sandbox has to carry the plain utilities the scripts use the way the host
	// would instead of failing one of them at a missing `dirname`.
	for _, command := range []string{
		"dirname", "mktemp", "head", "od", "tr", "grep", "cat", "rm", "date", "sleep", "chmod", "mkdir",
	} {
		real, err := exec.LookPath(command)
		if err != nil {
			continue
		}
		if err := os.Symlink(real, filepath.Join(binDir, command)); err != nil {
			t.Fatalf("link %s: %v", command, err)
		}
	}

	callLog := filepath.Join(dir, "calls.log")
	cmd := exec.Command(shPath, scriptPath)
	cmd.Dir = dir
	cmd.Env = []string{
		"PATH=" + binDir,
		"HOME=" + dir,
		"TMPDIR=" + dir,
		"CALL_LOG=" + callLog,
		// Every stub answers with success, so a retry could only delay the report.
		"TEST_MULTICA_KUBECTL_RETRIES=1",
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr

	exitCode := 0
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("run %s: %v", script, err)
		}
		exitCode = exitErr.ExitCode()
	}
	calls, readErr := os.ReadFile(callLog)
	if readErr != nil && !os.IsNotExist(readErr) {
		t.Fatalf("read call log: %v", readErr)
	}
	return entryRun{exitCode: exitCode, stderr: stderr.String(), calls: string(calls)}
}
