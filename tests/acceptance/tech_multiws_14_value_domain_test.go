// TC-tech-multiws-14 implements AC-101: the per-workspace in-flight cap has a
// value domain — absent leaves that level off, a positive value at or below the
// global cap arms it (equal is legal), and a non-positive value or one above
// the global cap is a configuration error. The refusal runs the real binary:
// the process exits non-zero before any client exists, so the controlled
// server observes no registration, no heartbeat and no claim, while the
// startup log names the offending value (plus the global cap when that is the
// bound it broke). The equal-to-global form starts and serves a claim.
package acceptance

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/scheduler"
)

// buildForemanBinary builds cmd/foreman for the refusal cases: the exit code
// is part of what AC-101 judges, and driving the loader alone would leave the
// "process exits, nothing serves" half unproven.
func buildForemanBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "foreman")
	build := exec.Command("go", "build", "-o", bin, "./cmd/foreman")
	build.Dir = "../.."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build ./cmd/foreman: %v\n%s", err, out)
	}
	return bin
}

// foremanStartupEnv is the environment a run starts with: the keys every
// loader before the scheduler needs, so the value under test is what decides
// the outcome. Ambient FOREMAN_*/MULTICA_* keys and KUBECONFIG are dropped so
// the caller's environment cannot change the verdict; an override replaces the
// base value, including with the empty string.
func foremanStartupEnv(serverURL string, overrides map[string]string) []string {
	env := make([]string, 0, len(os.Environ())+8)
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "FOREMAN_") || strings.HasPrefix(key, "MULTICA_") || key == "KUBECONFIG" {
			continue
		}
		env = append(env, entry)
	}
	merged := map[string]string{
		"MULTICA_SERVER_URL":    serverURL,
		"MULTICA_WORKSPACE_ID":  "ws-a",
		"MULTICA_TOKEN":         "mdt_ws-a",
		"FOREMAN_JOB_TOKEN_KEY": strings.Repeat("ab", 32),
	}
	for key, value := range overrides {
		merged[key] = value
	}
	for key, value := range merged {
		env = append(env, key+"="+value)
	}
	return env
}

type foremanExit struct {
	code           int
	stdout, stderr string
}

// runForeman starts the binary with the given overrides and waits for it to
// end; a run that outlives the window is a hang, not a verdict.
func runForeman(t *testing.T, bin, serverURL string, overrides map[string]string) foremanExit {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin)
	cmd.Dir = t.TempDir()
	cmd.Env = foremanStartupEnv(serverURL, overrides)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("foreman did not exit within the window; stderr:\n%s", stderr.String())
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return foremanExit{code: exitErr.ExitCode(), stdout: stdout.String(), stderr: stderr.String()}
	}
	if err != nil {
		t.Fatalf("run foreman: %v", err)
	}
	return foremanExit{stdout: stdout.String(), stderr: stderr.String()}
}

func TestTechMultiWS14PerWorkspaceCapDomain(t *testing.T) {
	bin := buildForemanBinary(t)

	for _, tc := range []struct {
		name    string
		value   string
		mustSay []string
	}{
		{
			name:  "above the global cap",
			value: "11",
			// The refusal names the value and the global cap it broke.
			mustSay: []string{scheduler.EnvMaxInflightPerWorkspace, "(11)", scheduler.EnvMaxInflightJobs, "(10)"},
		},
		{
			name:    "zero",
			value:   "0",
			mustSay: []string{scheduler.EnvMaxInflightPerWorkspace, `"0"`},
		},
		{
			name:    "negative",
			value:   "-1",
			mustSay: []string{scheduler.EnvMaxInflightPerWorkspace, `"-1"`},
		},
	} {
		t.Run(tc.name+" refuses startup", func(t *testing.T) {
			upstream := newUpstreamStub(t)
			run := runForeman(t, bin, upstream.server.URL, map[string]string{
				scheduler.EnvMaxInflightJobs:         "10",
				scheduler.EnvMaxInflightPerWorkspace: tc.value,
			})
			if run.code != 1 {
				t.Fatalf("exit code = %d, want 1 (a configuration error is fatal); stderr:\n%s", run.code, run.stderr)
			}
			for _, want := range tc.mustSay {
				if !strings.Contains(run.stderr, want) {
					t.Errorf("startup log does not name %q:\n%s", want, run.stderr)
				}
			}

			// The service face never opened: no runtime row, no heartbeat, no
			// claim — the controlled server saw no request at all.
			for _, path := range []string{
				"/api/daemon/register",
				"/api/daemon/heartbeat",
				"/api/daemon/tasks/claim",
			} {
				if calls := upstream.pathCalls(path); len(calls) != 0 {
					t.Errorf("%s reached the server %d times during a refused startup, want 0", path, len(calls))
				}
			}
			if all := upstream.all(); len(all) != 0 {
				t.Errorf("refused startup produced %d upstream requests, want none", len(all))
			}
		})
	}

	t.Run("equal to the global cap starts and serves", func(t *testing.T) {
		cfg, err := scheduler.LoadConfig(func(key string) string {
			switch key {
			case scheduler.EnvMaxInflightJobs:
				return "2"
			case scheduler.EnvMaxInflightPerWorkspace:
				return "2"
			default:
				return ""
			}
		})
		if err != nil {
			t.Fatalf("a per-workspace cap equal to the global cap was refused: %v", err)
		}
		if cfg.MaxInflightJobs != 2 || cfg.MaxInflightJobsPerWorkspace != 2 {
			t.Fatalf("caps = %d/%d, want 2/2", cfg.MaxInflightJobs, cfg.MaxInflightJobsPerWorkspace)
		}

		m := newMultiWS(t, "ws-a")
		m.rebuildScheduler(func(c *scheduler.Config) {
			c.MaxInflightJobs = cfg.MaxInflightJobs
			c.MaxInflightJobsPerWorkspace = cfg.MaxInflightJobsPerWorkspace
		})
		entry := m.claim("ws-a", "task-a1", "rid-a")
		if !m.jobs.jobCreated(entry.JobName) {
			t.Fatalf("Job %s was not created: the started deployment did not serve the claim", entry.JobName)
		}
		if got := m.sched.ClaimBudgetFor("ws-a"); got != 1 {
			t.Errorf("claim budget with one of two slots in flight = %d, want 1", got)
		}
	})
}
