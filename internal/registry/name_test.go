package registry

import (
	"regexp"
	"strings"
	"testing"
)

// testTaskID is a task id in the shape the platform issues (UUIDv7): the tail
// — not the front — is the random half, which is why the key takes the tail.
const testTaskID = "0199c0e4-1234-7abc-8def-550f91a80824"

func TestJobName(t *testing.T) {
	tests := []struct {
		name            string
		taskID          string
		issueIdentifier string
		agentName       string
		want            string
	}{
		{
			name:            "issue context",
			taskID:          testTaskID,
			issueIdentifier: "TSI-3862",
			agentName:       "Lynx",
			want:            "fm-tsi-3862-lynx-550f91a80824",
		},
		{
			// No issue context (autopilot / chat entry): the shared fallback
			// literal, not a per-entry split.
			name:      "no issue context",
			taskID:    testTaskID,
			agentName: "Lynx",
			want:      "fm-task-lynx-550f91a80824",
		},
		{
			// A payload without an agent object is not a rejection path.
			name:            "no agent name",
			taskID:          testTaskID,
			issueIdentifier: "TSI-3862",
			want:            "fm-tsi-3862-agent-550f91a80824",
		},
		{
			// Upper case, spaces and punctuation are normalized, not refused.
			name:            "agent name normalized",
			taskID:          testTaskID,
			issueIdentifier: "TSI-3862",
			agentName:       "Lynx The  Bot!",
			want:            "fm-tsi-3862-lynx-the-bot-550f91a80824",
		},
		{
			name:            "agent name with separators only",
			taskID:          testTaskID,
			issueIdentifier: "TSI-3862",
			agentName:       "  *  ",
			want:            "fm-tsi-3862-agent-550f91a80824",
		},
		{
			// Non-ASCII normalizes away entirely: the fallback keeps the name
			// derivable instead of failing the task.
			name:            "non-ascii agent name",
			taskID:          testTaskID,
			issueIdentifier: "TSI-3862",
			agentName:       "测试",
			want:            "fm-tsi-3862-agent-550f91a80824",
		},
		{
			name:            "agent name truncated to its budget",
			taskID:          testTaskID,
			issueIdentifier: "TSI-3862",
			agentName:       strings.Repeat("a", 40),
			want:            "fm-tsi-3862-" + strings.Repeat("a", agentSegmentMax) + "-550f91a80824",
		},
		{
			// Budgets are per segment, and the longest name stays a legal
			// label (3 + 20 + 1 + 24 + 1 + 12 = 61 ≤ 63).
			name:            "both segments at budget",
			taskID:          testTaskID,
			issueIdentifier: strings.Repeat("b", 40),
			agentName:       strings.Repeat("c", 40),
			want: "fm-" + strings.Repeat("b", issueSegmentMax) + "-" +
				strings.Repeat("c", agentSegmentMax) + "-550f91a80824",
		},
		{
			// Truncation must not leave a trailing dash behind.
			name:            "truncation drops the trailing dash",
			taskID:          testTaskID,
			issueIdentifier: "aaaa-bbbb-cccc-dddd-eeee",
			agentName:       "Lynx",
			want:            "fm-aaaa-bbbb-cccc-dddd-lynx-550f91a80824",
		},
		{
			// fm-catalog- is the probe's name domain: a task Job must never
			// shadow it, so the reserved literal falls back.
			name:            "reserved issue segment",
			taskID:          testTaskID,
			issueIdentifier: "CATALOG",
			agentName:       "Lynx",
			want:            "fm-task-lynx-550f91a80824",
		},
		{
			name:            "issue identifier normalized",
			taskID:          testTaskID,
			issueIdentifier: "  tsi/3862 ",
			agentName:       "Lynx",
			want:            "fm-tsi-3862-lynx-550f91a80824",
		},
		{
			// Short ids keep whatever tail they have: the key is the id minus
			// its dashes, truncated from the front.
			name:      "short task id",
			taskID:    "task-1",
			agentName: "Lynx",
			want:      "fm-task-lynx-task1",
		},
		{
			name:            "task id keeps only its tail",
			taskID:          "0199c0e4-1234-7abc-8def-550f91a8082400",
			issueIdentifier: "TSI-3862",
			agentName:       "Lynx",
			want:            "fm-tsi-3862-lynx-0f91a8082400",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := JobName(test.taskID, test.issueIdentifier, test.agentName)
			if got != test.want {
				t.Errorf("JobName(%q, %q, %q) = %q, want %q",
					test.taskID, test.issueIdentifier, test.agentName, got, test.want)
			}
		})
	}
}

// TestJobNameUnderivable pins the one failing input class: a task id whose
// characters cannot form a DNS-1123 label has no derivable name, and the
// scheduler settles that task as invalid_task_id (failure-handling #0) rather
// than creating a Job the apiserver would reject.
func TestJobNameUnderivable(t *testing.T) {
	for _, taskID := range []string{"", "-", "0199c0e4_1234", "task/1", "TSI 3862"} {
		if got := JobName(taskID, "TSI-3862", "Lynx"); got != "" {
			t.Errorf("JobName(%q) = %q, want \"\"", taskID, got)
		}
	}
}

// TestJobNameIsALabelTheCapTolerates guards the two hard limits the name must
// satisfy at once: it is the Job's metadata.name and the pod's job-name label
// value, hence a ≤63-character DNS-1123 label, for every reachable input —
// including the worst case (both segments at their budget) and non-ASCII
// names.
func TestJobNameIsALabelTheCapTolerates(t *testing.T) {
	label := regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	inputs := []struct{ issue, agent string }{
		{"", ""},
		{strings.Repeat("i", 64), strings.Repeat("a", 64)},
		{"测试", "测试"},
		{"TSI-3862", "Lynx"},
	}
	for _, in := range inputs {
		name := JobName(testTaskID, in.issue, in.agent)
		if name == "" {
			t.Fatalf("JobName(%q, %q) is empty, want a name", in.issue, in.agent)
		}
		if len(name) > 63 || !label.MatchString(name) {
			t.Errorf("JobName(%q, %q) = %q: not a ≤63-character DNS-1123 label", in.issue, in.agent, name)
		}
	}
}

// TestJobNameIsStableAndOneToOne pins the property the redispatch path depends
// on: one task, one name — the same inputs always answer the same name, and
// two different tasks of the same agent never share one.
func TestJobNameIsStableAndOneToOne(t *testing.T) {
	first := JobName(testTaskID, "TSI-3862", "Lynx")
	if again := JobName(testTaskID, "TSI-3862", "Lynx"); again != first {
		t.Errorf("JobName is not stable: %q then %q", first, again)
	}
	other := JobName("0199c0e4-1234-7abc-8def-550f91a80825", "TSI-3862", "Lynx")
	if other == first {
		t.Errorf("two tasks share the name %q", first)
	}
}

func TestCredName(t *testing.T) {
	if got, want := CredName("fm-tsi-3862-lynx-550f91a80824"), "fm-tsi-3862-lynx-550f91a80824-cred"; got != want {
		t.Errorf("CredName() = %q, want %q", got, want)
	}
}
