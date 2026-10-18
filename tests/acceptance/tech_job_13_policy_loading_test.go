// TC-tech-job-13 implements AC-44: the policy file's loading and failure
// surface. A missing file is a legal default (three axes None, warn, boot
// continues); an unreadable, unparseable or drop-writing file refuses startup
// with its own class and the file's hash evidence; a dead definition is
// visible but never blocks; and the file is read once — an edit after load
// changes nothing. The cases drive jobbuilder.NewBuilder directly, which is
// the same startup point main runs before it serves.
package acceptance

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/observability"
)

// tcJob13Declaration is the fixed declaration sentence every policy-side
// startup path prints (02-contracts.md §8.7 风险告知).
const tcJob13Declaration = "策略文件在启动期一次性加载，运行中变更不生效"

// tcJob13HashPattern is the hash line's shape: the prefix and 64 lowercase hex
// digits, nothing else.
var tcJob13HashPattern = regexp.MustCompile(`策略文件内容哈希 sha256=([0-9a-f]{64})\b`)

// tcPolicyAxisConfig builds the module configuration a startup-gate case runs
// with: the template and policy carriers, the seams, and a log the case can
// read.
func tcPolicyAxisConfig(t *testing.T, templatePath, policyPath string, mutate func(*jobbuilder.Config)) (jobbuilder.Config, *bytes.Buffer) {
	t.Helper()
	cfg, err := jobbuilder.LoadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	cfg.TemplatePath = templatePath
	cfg.PolicyPath = policyPath
	cfg.JobImage = harnessJobImage
	cfg.JobNamespace = "multica-agents"
	cfg.Issuer = stubIssuer{}
	logs := &bytes.Buffer{}
	cfg.Logger = observability.NewLogger(logs, slog.LevelInfo, "")
	if mutate != nil {
		mutate(&cfg)
	}
	return cfg, logs
}

// tcPolicyAxisBuilder runs the production startup gate over a template/policy
// pair and returns the builder with the startup log it wrote.
func tcPolicyAxisBuilder(t *testing.T, templatePath, policyPath string, mutate func(*jobbuilder.Config)) (*jobbuilder.Builder, *bytes.Buffer) {
	t.Helper()
	cfg, logs := tcPolicyAxisConfig(t, templatePath, policyPath, mutate)
	builder, err := jobbuilder.NewBuilder(cfg)
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	return builder, logs
}

// TestJob13MissingFileIsALegalDefault is AC-44's first clause: the policy file
// is an optional carrier, so its absence reads every axis None, prints the
// declaration and the warn, and never refuses the boot — unlike the template's
// absence, which refuses to serve.
func TestJob13MissingFileIsALegalDefault(t *testing.T) {
	policyPath := filepath.Join(t.TempDir(), "policy.yaml")
	builder, logs := tcPolicyAxisBuilder(t, deployedTemplatePath(t), policyPath, nil)
	log := logs.String()

	if !strings.Contains(log, tcJob13Declaration) {
		t.Errorf("the missing-file path must print the declaration sentence: %s", log)
	}
	if !strings.Contains(log, "策略文件缺席") || !strings.Contains(log, "三轴按 None 读") {
		t.Errorf("the missing file must be recorded as the legal default: %s", log)
	}
	if strings.Contains(log, "策略文件内容哈希") {
		t.Errorf("a file that was never read has no hash to report: %s", log)
	}

	// The three axes read None: the template survives verbatim.
	job, _, _, err := builder.Build(jobbuilder.TaskEntry{
		TaskID: "task-1", AgentID: "agent-1", WorkspaceID: "ws-1",
		JobRuntimeID: "rid-1", ClaimedAt: testClaimTime, Attempt: 1, AgentName: "omp",
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	tcPolicyAxisWantResources(t, job.Spec.Template.Spec.Containers[0].Resources,
		tcPolicyAxisTemplateRequests, tcPolicyAxisTemplateLimits)
	tcPolicyAxisWantSet(t, "containers[0].securityContext.capabilities.drop", tcPolicyAxisDrop(job), "ALL")
	if add := tcPolicyAxisAdd(job); len(add) != 0 {
		t.Errorf("containers[0] capabilities.add = %v, want none", add)
	}
	tcPolicyAxisWantNodeSelector(t, job.Spec.Template.Spec, map[string]string{"kubernetes.io/os": "linux"})
}

// TestJob13FailureClassesRefuseStartup is AC-44's second clause: each bad file
// shape refuses the boot as its own class, and the readable ones carry the
// file's hash while the unreadable one states the hash is out of reach.
func TestJob13FailureClassesRefuseStartup(t *testing.T) {
	cases := []struct {
		name         string
		path         func(t *testing.T) string
		class        string
		readable     bool
		wantSentinel bool
	}{
		{
			// The path exists but reading it fails: a directory is the
			// portable stand-in, registered through Config.PolicyPath.
			name:  "file unreadable",
			path:  func(t *testing.T) string { return t.TempDir() },
			class: "文件不可读",
		},
		{
			name:         "not parseable as YAML",
			path:         func(t *testing.T) string { return writePolicyFile(t, "agents: [\n") },
			class:        "not parseable as YAML",
			readable:     true,
			wantSentinel: true,
		},
		{
			name: "unknown key refused by strict decoding",
			path: func(t *testing.T) string {
				return writePolicyFile(t, "agents:\n  omp:\n    resourcePolcy: agent\n")
			},
			class:        "unknown field",
			readable:     true,
			wantSentinel: true,
		},
		{
			name: "definition writes drop",
			path: func(t *testing.T) string {
				return writePolicyFile(t, "agents:\n  omp:\n    capPolicy: agent\n    capabilities:\n      add: [\"NET_ADMIN\"]\n      drop: [\"ALL\"]\n")
			},
			class:        ".drop is not writable",
			readable:     true,
			wantSentinel: true,
		},
		{
			name: "tier literal is not one of the four",
			path: func(t *testing.T) string {
				return writePolicyFile(t, "agents:\n  omp:\n    resourcePolicy: Project\n")
			},
			class:        "is not one of None/project/agent/projectWithAgent",
			readable:     true,
			wantSentinel: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := tc.path(t)
			cfg, logs := tcPolicyAxisConfig(t, deployedTemplatePath(t), path, nil)

			builder, err := jobbuilder.NewBuilder(cfg)
			if err == nil {
				t.Fatalf("NewBuilder accepted a broken policy file: %+v", builder)
			}
			// The refusal names the carrier: the log line (asserted below)
			// carries the path, which is the class-separation evidence the
			// design asks for.
			if tc.wantSentinel && !errors.Is(err, jobbuilder.ErrInvalidPolicy) {
				t.Errorf("error %v must be ErrInvalidPolicy", err)
			}

			log := logs.String()
			if !strings.Contains(log, tcJob13Declaration) {
				t.Errorf("the refusal must carry the declaration sentence: %s", log)
			}
			if !strings.Contains(log, tc.class) {
				t.Errorf("the refusal log must name the class (%q): %s", tc.class, log)
			}
			if !strings.Contains(log, path) {
				t.Errorf("the refusal log must carry the path: %s", log)
			}
			if !tc.readable {
				if !strings.Contains(log, "策略文件内容哈希不可得") {
					t.Errorf("the unreadable path must state the hash is out of reach: %s", log)
				}
				if strings.Contains(log, "策略文件内容哈希 sha256=") {
					t.Errorf("the unreadable path has no hash to report: %s", log)
				}
				return
			}
			// The readable paths report the bytes the process loaded.
			content, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read policy: %v", err)
			}
			sum := sha256.Sum256(content)
			want := "策略文件内容哈希 sha256=" + hex.EncodeToString(sum[:])
			if !strings.Contains(log, want) {
				t.Errorf("the refusal log must carry %s: %s", want, log)
			}
			matches := tcJob13HashPattern.FindAllStringSubmatch(log, -1)
			if len(matches) == 0 || matches[0][1] != hex.EncodeToString(sum[:]) {
				t.Errorf("the hash line must be the file's own sha256 in lowercase hex: %s", log)
			}
		})
	}
}

// TestJob13DeadDefinitionWarnsAndStarts is AC-44's dead-configuration clause: a
// definition no axis references is visible (warn) but never blocking — and a
// definition an axis does reference is not reported.
func TestJob13DeadDefinitionWarnsAndStarts(t *testing.T) {
	t.Run("unreferenced definitions are reported", func(t *testing.T) {
		policyPath := writePolicyFile(t, `
agents:
  omp:
    resources:
      limits: { cpu: "2" }
projects:
  proj-1:
    nodeSelector: { disktype: ssd }
`)
		builder, logs := tcPolicyAxisBuilder(t, deployedTemplatePath(t), policyPath, nil)
		if builder == nil {
			t.Fatal("the dead configuration must not refuse the boot")
		}
		log := logs.String()
		if !strings.Contains(log, "job policy carries definitions no axis references") {
			t.Errorf("the dead configuration must be reported: %s", log)
		}
		for _, key := range []string{"agents.omp.resources", "projects.proj-1.nodeSelector"} {
			if !strings.Contains(log, key) {
				t.Errorf("the dead warning must name %q: %s", key, log)
			}
		}
	})

	t.Run("referenced definitions are not reported", func(t *testing.T) {
		policyPath := writePolicyFile(t, `
agents:
  omp:
    resourcePolicy: agent
    nodeSelectorPolicy: project
    resources:
      limits: { cpu: "2" }
projects:
  proj-1:
    nodeSelector: { disktype: ssd }
`)
		builder, logs := tcPolicyAxisBuilder(t, deployedTemplatePath(t), policyPath, nil)
		if builder == nil {
			t.Fatal("a referenced definition must not refuse the boot")
		}
		if log := logs.String(); strings.Contains(log, "definitions no axis references") {
			t.Errorf("a definition the agent tier references is not dead: %s", log)
		}
	})
}

// TestJob13EditingTheFileAfterLoadHasNoEffect is AC-44's no-hot-reload clause
// (ADR-035 decision 7): the table is read once at startup, so a second build
// after the file changed still renders the first bytes.
func TestJob13EditingTheFileAfterLoadHasNoEffect(t *testing.T) {
	policyPath := writePolicyFile(t, `
agents:
  omp:
    capPolicy: agent
    capabilities:
      add: ["SYS_ADMIN"]
`)
	builder, _ := tcPolicyAxisBuilder(t, deployedTemplatePath(t), policyPath, nil)

	// The file changes after the load: the running process must not see it.
	if err := os.WriteFile(policyPath, []byte(`
agents:
  omp:
    capPolicy: agent
    capabilities:
      add: ["NET_ADMIN"]
`), 0o600); err != nil {
		t.Fatalf("rewrite policy: %v", err)
	}

	for _, taskID := range []string{"task-1", "task-2"} {
		job, _, _, err := builder.Build(jobbuilder.TaskEntry{
			TaskID: taskID, AgentID: "agent-1", WorkspaceID: "ws-1",
			JobRuntimeID: "rid-1", ClaimedAt: testClaimTime, Attempt: 1, AgentName: "omp",
		})
		if err != nil {
			t.Fatalf("Build(%s): %v", taskID, err)
		}
		_, add := tcPolicyAxisCapabilities(job)
		tcPolicyAxisWantSet(t, "containers[0].securityContext.capabilities.add", add, "SYS_ADMIN")
	}
}

// TestJob13PositiveSampleStartsAndHits is AC-44's positive control: a file that
// is present and legal starts, prints both the declaration and its own hash,
// and its definitions reach the rendered Job.
func TestJob13PositiveSampleStartsAndHits(t *testing.T) {
	policyPath := writePolicyFile(t, `
agents:
  omp:
    resourcePolicy: agent
    capPolicy: agent
    nodeSelectorPolicy: agent
    resources:
      requests: { cpu: "2" }
      limits:   { cpu: "4" }
    capabilities:
      add: ["NET_ADMIN"]
    nodeSelector: { disktype: ssd }
`)
	builder, logs := tcPolicyAxisBuilder(t, deployedTemplatePath(t), policyPath, nil)
	log := logs.String()
	if !strings.Contains(log, tcJob13Declaration) {
		t.Errorf("the accepting path must print the declaration sentence: %s", log)
	}
	content, err := os.ReadFile(policyPath)
	if err != nil {
		t.Fatalf("read policy: %v", err)
	}
	sum := sha256.Sum256(content)
	wantHash := "策略文件内容哈希 sha256=" + hex.EncodeToString(sum[:])
	if !strings.Contains(log, wantHash) {
		t.Errorf("the accepting path must record the file's hash %s: %s", wantHash, log)
	}

	job, _, _, err := builder.Build(jobbuilder.TaskEntry{
		TaskID: "task-1", AgentID: "agent-1", WorkspaceID: "ws-1",
		JobRuntimeID: "rid-1", ClaimedAt: testClaimTime, Attempt: 1, AgentName: "omp",
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	tcPolicyAxisWantResources(t, job.Spec.Template.Spec.Containers[0].Resources,
		map[string]string{"cpu": "2"}, map[string]string{"cpu": "4"})
	tcPolicyAxisWantSet(t, "containers[0].securityContext.capabilities.add", tcPolicyAxisAdd(job), "NET_ADMIN")
	tcPolicyAxisWantNodeSelector(t, job.Spec.Template.Spec,
		map[string]string{"kubernetes.io/os": "linux", "disktype": "ssd"})
}
