// TC-tech-job-06 implements the two faces AC-27 names for the template: the
// startup gate's three failure classes (the file does not exist / cannot be
// read / is not a batch/v1 Job) with their per-class log lines, and the
// template-verbatim negative — an adversarial template starts. AC-28's
// injection face rides along: the three env keys are Foreman's, the other
// twelve are the template's. The process-layer half of the container-layer
// pair (the non-optional volume) is a deployment-spec assertion that needs a
// cluster; it stays QA-side.
package acceptance

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/observability"
)

// startupRefusal is one failure class of the startup gate: the class literal
// its log line must carry, and whether the content was readable — which is what
// decides the hash line (readable) versus the "hash unavailable" statement
// (the unreadable class only; a missing file has nothing to say about a hash).
type startupRefusal struct {
	name           string
	path           func(t *testing.T) string
	class          string
	wantHash       bool
	wantHashAbsent bool
}

func TestJob06StartupGateRefusesThreeClasses(t *testing.T) {
	cases := []startupRefusal{
		{
			// Class 1: the ConfigMap is there but has no job-overlay.yaml key.
			name:  "file missing",
			path:  func(t *testing.T) string { return filepath.Join(t.TempDir(), "job-overlay.yaml") },
			class: "模板文件不存在",
		},
		{
			// Class 2: the file exists but cannot be read — a directory is the
			// portable stand-in, and the design case registers this path as
			// reachable through Config.TemplatePath alone.
			name:           "file unreadable",
			path:           func(t *testing.T) string { return t.TempDir() },
			class:          "文件不可读",
			wantHashAbsent: true,
		},
		{
			name:     "yaml unparsable",
			path:     manifestPath("apiVersion: batch/v1\nkind: Job\nspec: [\n"),
			class:    "not parseable as YAML",
			wantHash: true,
		},
		{
			name:     "kind is not Job",
			path:     manifestPath("apiVersion: batch/v1\nkind: Deployment\n"),
			class:    "kind Job",
			wantHash: true,
		},
		{
			name:     "unknown field",
			path:     manifestPath("apiVersion: batch/v1\nkind: Job\nspec:\n  template:\n    spec:\n      contianers: []\n"),
			class:    "contianers",
			wantHash: true,
		},
		{
			// A $ prefixed key is an unknown field in this direction: there is
			// no merge step for it to address.
			name:     "dollar prefixed key",
			path:     manifestPath("apiVersion: batch/v1\nkind: Job\nspec:\n  template:\n    spec:\n      containers:\n        - name: agent\n          $patch: replace\n"),
			class:    "$patch",
			wantHash: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := tc.path(t)
			logs := &bytes.Buffer{}
			cfg, err := jobbuilder.LoadConfig(func(string) string { return "" })
			if err != nil {
				t.Fatalf("LoadConfig: %v", err)
			}
			cfg.TemplatePath = path
			cfg.Logger = observability.NewLogger(logs, slog.LevelInfo, "")

			// The call main makes before it serves: an error here is the
			// non-zero exit (CrashLoopBackOff under the Deployment), so no Job
			// is ever created for the task queue.
			builder, err := jobbuilder.NewBuilder(cfg)
			if err == nil {
				t.Fatalf("NewBuilder accepted the template: %+v", builder)
			}
			if !strings.Contains(err.Error(), path) {
				t.Errorf("error %q must name the path", err)
			}
			log := logs.String()
			if !strings.Contains(log, "Foreman 不对 Job 模板做任何校验，模板内容不构成安全边界") {
				t.Errorf("the refusal must carry the declaration: %s", log)
			}
			if !strings.Contains(log, tc.class) {
				t.Errorf("the refusal log must name the class (%q): %s", tc.class, log)
			}
			if !strings.Contains(log, path) {
				t.Errorf("the refusal log must carry the path: %s", log)
			}
			hashLine := "模板内容哈希 sha256="
			if tc.wantHashAbsent && !strings.Contains(log, "模板内容哈希不可得") {
				t.Errorf("the unreadable path must state the hash is out of reach: %s", log)
			}
			if !tc.wantHash {
				if strings.Contains(log, hashLine) {
					t.Errorf("an unreadable path has no hash to report: %s", log)
				}
				return
			}
			// The readable paths report the file's own bytes.
			content, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read template: %v", err)
			}
			sum := sha256.Sum256(content)
			if want := hashLine + hex.EncodeToString(sum[:]); !strings.Contains(log, want) {
				t.Errorf("the refusal log must carry %s: %s", want, log)
			}
		})
	}
}

// TestJob06AdversarialTemplateStarts is the negative of the classes above: the
// template the old direction refused wholesale (privileged, rewritten
// command/args, two containers, hostNetwork, its own serviceAccountName and
// parallelism) is accepted and renders a Job. Anything that refuses it is a
// regression to a shape checker this direction deleted.
func TestJob06AdversarialTemplateStarts(t *testing.T) {
	cfg, err := jobbuilder.LoadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	cfg.TemplatePath = writeTemplateFile(t, customWindowTemplate)
	cfg.Issuer = stubIssuer{}
	cfg.Logger = observability.NewLogger(&bytes.Buffer{}, slog.LevelInfo, "")

	builder, err := jobbuilder.NewBuilder(cfg)
	if err != nil {
		t.Fatalf("NewBuilder refused the adversarial template: %v", err)
	}
	job, _, _, err := builder.Build(jobbuilder.TaskEntry{
		TaskID: "task-1", AgentID: "agent-1", WorkspaceID: "ws-1",
		JobRuntimeID: "rid-1", ClaimedAt: testClaimTime, Attempt: 1,
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if job.Spec.Template.Spec.HostNetwork != true || len(job.Spec.Template.Spec.Containers) != 2 {
		t.Errorf("rendered Job = hostNetwork %v with %d containers, want the template's shape",
			job.Spec.Template.Spec.HostNetwork, len(job.Spec.Template.Spec.Containers))
	}
}

// TestJob06InjectsThreeEnvKeysOnly (AC-28): containers[0]'s three keys are
// Foreman's — a same-name entry is replaced whole, never duplicated — the
// remaining twelve template-side env keys are not injected, and a second
// container is outside the injection face.
func TestJob06InjectsThreeEnvKeysOnly(t *testing.T) {
	const manifest = `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<task_id>
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: first
          image: <job-image>
          env:
            - { name: MULTICA_SERVER_URL, value: "http://real-server.example" }
            - { name: MULTICA_DAEMON_ID, value: "static-id" }
            - { name: MULTICA_DAEMON_MAX_CONCURRENT_TASKS, value: "4" }
        - name: second
          image: busybox
`
	cfg, err := jobbuilder.LoadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	cfg.TemplatePath = writeTemplateFile(t, manifest)
	cfg.Issuer = stubIssuer{}
	cfg.Logger = observability.NewLogger(&bytes.Buffer{}, slog.LevelInfo, "")

	builder, err := jobbuilder.NewBuilder(cfg)
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	job, _, _, err := builder.Build(jobbuilder.TaskEntry{
		TaskID: "task-1", AgentID: "agent-1", WorkspaceID: "ws-1",
		JobRuntimeID: "rid-1", ClaimedAt: testClaimTime, Attempt: 1,
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	containers := job.Spec.Template.Spec.Containers
	if len(containers) != 2 {
		t.Fatalf("containers = %d, want the template's two", len(containers))
	}
	first := containers[0].Env
	if got := envValue(first, "MULTICA_SERVER_URL"); got != "http://foreman.foreman.svc.cluster.local:8080" {
		t.Errorf("MULTICA_SERVER_URL = %q, want Foreman's address", got)
	}
	daemonID := envByName(t, first, "MULTICA_DAEMON_ID")
	if daemonID.Value != "" || daemonID.ValueFrom == nil || daemonID.ValueFrom.FieldRef == nil ||
		daemonID.ValueFrom.FieldRef.FieldPath != "metadata.labels['foreman.tsic.top/daemon-id']" {
		t.Errorf("MULTICA_DAEMON_ID = %+v, want the fieldRef only", daemonID)
	}
	if got := envValue(first, "MULTICA_DAEMON_MAX_CONCURRENT_TASKS"); got != "1" {
		t.Errorf("MULTICA_DAEMON_MAX_CONCURRENT_TASKS = %q, want 1", got)
	}
	seen := map[string]int{}
	for _, entry := range first {
		seen[entry.Name]++
	}
	for name, count := range seen {
		if count != 1 {
			t.Errorf("env %q appears %d times, want one entry", name, count)
		}
	}
	// The reference template's other env keys: none of the rest may appear.
	for _, name := range []string{
		"MULTICA_DAEMON_DEVICE_NAME", "MULTICA_AGENT_RUNTIME_NAME", "MULTICA_WORKSPACES_ROOT",
		"TMPDIR", "TMP", "TEMP", "MULTICA_GC_ENABLED", "MULTICA_DAEMON_AUTO_UPDATE",
		"MULTICA_DAEMON_AUTO_RELOAD", "MULTICA_OMP_PATH", "HOME", "LOG_LEVEL",
	} {
		if _, present := seen[name]; present {
			t.Errorf("env %q was injected; only the three A1.1 keys are Foreman's", name)
		}
	}
	for _, entry := range containers[1].Env {
		if strings.HasPrefix(entry.Name, "MULTICA_") {
			t.Errorf("containers[1] carries %q: the injection targets containers[0]", entry.Name)
		}
	}
}

// manifestPath writes one manifest to a template file and returns its path.
func manifestPath(manifest string) func(t *testing.T) string {
	return func(t *testing.T) string { return writeTemplateFile(t, manifest) }
}

// testClaimTime is the claim instant these startup-gate builds carry.
var testClaimTime = time.Date(2026, 10, 4, 8, 30, 0, 0, time.UTC)

// stubIssuer is the jobbuilder.TokenIssuer seam: the startup-gate cases assert
// the gate and the render, never a credential.
type stubIssuer struct{}

func (stubIssuer) Issue(string, string, string, time.Duration) (string, error) {
	return "fmj_test.token", nil
}

func (stubIssuer) IssueProbe(string, string, string, time.Duration) (string, error) {
	return "fmj_test.probe", nil
}

func (stubIssuer) Hash(token string) string { return "hash(" + token + ")" }
