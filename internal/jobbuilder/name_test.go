package jobbuilder

import (
	"errors"
	"strings"
	"testing"
)

// TestValidateNameFormat: the startup rules of ADR-034 decision 3 — the
// character-level closure, the mandatory unique task segment, the worst-case
// length budget and the minimum-value DNS-1123 shape. A non-empty format that
// breaks any of them refuses startup; only an empty format falls back.
func TestValidateNameFormat(t *testing.T) {
	cases := []struct {
		name   string
		format string
		wantOK bool
	}{
		{"empty falls back", "", true},
		{"identity format", "fm-<task_id>", true},
		{"readable format", "fm-<issue-segment>-<agent-segment>-<task-key>", true},
		{"task key alone", "<task-key>", true},
		{"static name has no unique segment", "fm-static", false},
		{"raw value variable", "fm-<issue-identifier>-<task-key>", false},
		{"registered but not a name variable", "fm-<job-name>", false},
		{"identity placeholder is not a name variable", "fm-<daemon-id>", false},
		{"leading dash", "-<task-key>", false},
		{"trailing dash", "<task-key>-", false},
		{"uppercase literal", "FM-<task_key_or_id>", false},
		{"underscore literal", "fm_<task-key>", false},
		{"unclosed variable", "fm-<task_id", false},
		{"worst case over 63", "fm-<issue-segment>-<agent-segment>-<task-key>-<task_id>", false},
		{"worst case exactly 63", strings.Repeat("a", 3) + "-<task_id>-" + strings.Repeat("a", 22), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateNameFormat(tc.format)
			if tc.wantOK && err != nil {
				t.Fatalf("validateNameFormat(%q) = %v, want nil", tc.format, err)
			}
			if !tc.wantOK {
				if err == nil {
					t.Fatalf("validateNameFormat(%q) = nil, want a refusal", tc.format)
				}
				if !errors.Is(err, ErrInvalidJobNameFormat) {
					t.Errorf("error = %v, want ErrInvalidJobNameFormat", err)
				}
			}
		})
	}
}

// TestNormalizeSegment: lowercase, fold every run of non-[a-z0-9] into one
// dash, trim, truncate to the budget, trim a dash the truncation exposed, and
// fall back when nothing is left.
func TestNormalizeSegment(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		budget   int
		fallback string
		want     string
	}{
		{"identifier", "TSI-3664", budgetIssueSegment, fallbackIssueSegment, "tsi-3664"},
		{"spaces fold", "Hello  World", budgetAgentSegment, fallbackAgentSegment, "hello-world"},
		{"punctuation runs fold", "  --Foo__Bar--  ", budgetAgentSegment, fallbackAgentSegment, "foo-bar"},
		{"non-ascii disappears", "任务一", budgetAgentSegment, fallbackAgentSegment, fallbackAgentSegment},
		{"empty falls back", "", budgetAgentSegment, fallbackAgentSegment, fallbackAgentSegment},
		{"only separators falls back", "---", budgetIssueSegment, fallbackIssueSegment, fallbackIssueSegment},
		{"truncated to budget", strings.Repeat("a", 30), budgetIssueSegment, fallbackIssueSegment, strings.Repeat("a", budgetIssueSegment)},
		{"truncation trims the exposed dash", strings.Repeat("a", 19) + "-b", budgetIssueSegment, fallbackIssueSegment, strings.Repeat("a", 19)},
		{"inner dashes survive", "a-b-c", budgetIssueSegment, fallbackIssueSegment, "a-b-c"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeSegment(tc.raw, tc.budget, tc.fallback); got != tc.want {
				t.Errorf("normalizeSegment(%q, %d) = %q, want %q", tc.raw, tc.budget, got, tc.want)
			}
		})
	}
}

// TestTaskKeyOf: the unique task segment is the dashes-stripped task id's last
// twelve characters — 48 bits, which the design accepts as the collision bound.
func TestTaskKeyOf(t *testing.T) {
	cases := map[string]string{
		"3f6b1a2c-9d4e-4a5b-8c7d-0e1f2a3b4c5d": "0e1f2a3b4c5d",
		"abcdef":                               "abcdef",
	}
	for taskID, want := range cases {
		if got := taskKeyOf(taskID); got != want {
			t.Errorf("taskKeyOf(%q) = %q, want %q", taskID, got, want)
		}
	}
}

// TestResolveName: the template's format string is the name source; an absent
// or empty one falls back to the identity name. The rendered value is what the
// Job carries — never a rewritten version of the deployer's format.
func TestResolveName(t *testing.T) {
	t.Run("readable format renders segments", func(t *testing.T) {
		cfg, _ := templateConfig(t, `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<issue-segment>-<agent-segment>-<task-key>
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: agent
          image: <job-image>
`)
		entry := testEntry()
		entry.AgentName = "Vulcan Ops"
		entry.IssueIdentifier = "TSI-3870"
		builder := newTestBuilder(t, cfg)
		name, err := builder.ResolveName(entry)
		if err != nil {
			t.Fatalf("ResolveName: %v", err)
		}
		if want := "fm-tsi-3870-vulcan-ops-0e1f2a3b4c5d"; name != want {
			t.Errorf("name = %q, want %q", name, want)
		}
		job, _ := mustBuild(t, cfg, entry)
		if job.Name != name {
			t.Errorf("job name = %q, want the resolved %q", job.Name, name)
		}
	})

	t.Run("no format falls back to the identity name", func(t *testing.T) {
		cfg, _ := templateConfig(t, `
apiVersion: batch/v1
kind: Job
metadata:
  namespace: template-ns
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: agent
          image: <job-image>
`)
		entry := testEntry()
		builder := newTestBuilder(t, cfg)
		name, err := builder.ResolveName(entry)
		if err != nil {
			t.Fatalf("ResolveName: %v", err)
		}
		if name != daemonIDFor(entry.TaskID) {
			t.Errorf("name = %q, want the fallback %q", name, daemonIDFor(entry.TaskID))
		}
		if got := builder.nameSource(); got != "fallback" {
			t.Errorf("name source = %q, want fallback", got)
		}
	})

	t.Run("task_key floor keeps two tasks apart", func(t *testing.T) {
		cfg, _ := templateConfig(t, `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<task-key>
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: agent
          image: <job-image>
`)
		builder := newTestBuilder(t, cfg)
		first, err := builder.ResolveName(TaskEntry{TaskID: "aaaaaaaa-1111-4111-8111-111111111111"})
		if err != nil {
			t.Fatalf("ResolveName: %v", err)
		}
		second, err := builder.ResolveName(TaskEntry{TaskID: "aaaaaaaa-2222-4222-8222-222222222222"})
		if err != nil {
			t.Fatalf("ResolveName: %v", err)
		}
		if first == second {
			t.Errorf("both tasks resolved to %q, want distinct names", first)
		}
	})
}

// TestLoadJobTemplateRefusesIllegalNameFormat: the name format is failure class
// 4 — the file is readable, so the log carries its hash, and no Builder exists
// afterwards (there is no silent fallback).
func TestLoadJobTemplateRefusesIllegalNameFormat(t *testing.T) {
	cfg, logs := templateConfig(t, `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-static
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: agent
          image: <job-image>
`)
	_, err := LoadJobTemplate(cfg)
	if err == nil {
		t.Fatal("LoadJobTemplate accepted a static name, want a refusal")
	}
	if !errors.Is(err, ErrInvalidJobNameFormat) {
		t.Errorf("error = %v, want ErrInvalidJobNameFormat", err)
	}
	if out := logs.String(); !strings.Contains(out, templateHashLinePrefix) {
		t.Errorf("readable-file refusal must carry the hash line, got:\n%s", out)
	}
}
