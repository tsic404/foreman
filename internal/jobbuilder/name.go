package jobbuilder

import (
	"errors"
	"fmt"
	"strings"
)

// The name face (ADR-034, 03-contracts.md §5.4「名字合成」): the template's
// metadata.name is the only source of the Job's display name. It is a format
// string over a character-level closure — four variables plus lowercase
// letters, digits and '-' — and Foreman only falls back to the identity name
// fm-<task_id> when the template writes none. Identity (daemon_id, the
// credential Secret, the token's j claim) never follows the display name.
//
// The name is not a locator: rebuild, deletion, Secret sweeps and pod
// ownership all key on the foreman.tsic.top/task-id label (contract §1.3).
const (
	// Name variables, only valid inside metadata.name. Anywhere else they stay
	// verbatim like any other unregistered <...> literal.
	nameVarIssueSegment = "<issue-segment>"
	nameVarAgentSegment = "<agent-segment>"
	nameVarTaskKey      = "<task-key>"
	nameVarTaskID       = "<task_id>"

	// Segment budgets (ADR-034 decision 4) and the task-id budget of the
	// worst-case length arithmetic.
	budgetIssueSegment = 20
	budgetAgentSegment = 24
	budgetTaskKey      = 12
	budgetTaskID       = 36

	// fallbackIssueSegment / fallbackAgentSegment are what an empty
	// normalization yields.
	fallbackIssueSegment = "task"
	fallbackAgentSegment = "agent"

	// maxJobNameLength is the DNS-1123 label limit for a Job name.
	maxJobNameLength = 63
)

// ErrInvalidJobNameFormat marks a template whose metadata.name is not a legal
// name format: the startup gate refuses it (failure class 4), and the render
// path's final check fails the task as invalid_task_id.
var ErrInvalidJobNameFormat = errors.New("invalid job name format")

// nameVariables maps each name-face variable to its worst-case and
// minimum-value renderings. The worst case feeds the length arithmetic; the
// minimum feeds the DNS-1123 shape check (a one-character segment is the
// shortest a normalized segment can be, so a format whose minimum rendering is
// not DNS-1123 can never render a legal name).
type nameVariable struct {
	literal  string
	worst    string
	minValue string
}

var nameVariables = []nameVariable{
	{literal: nameVarIssueSegment, worst: strings.Repeat("a", budgetIssueSegment), minValue: "a"},
	{literal: nameVarAgentSegment, worst: strings.Repeat("a", budgetAgentSegment), minValue: "a"},
	{literal: nameVarTaskKey, worst: strings.Repeat("a", budgetTaskKey), minValue: "a"},
	{literal: nameVarTaskID, worst: strings.Repeat("a", budgetTaskID), minValue: "a"},
}

// validateNameFormat applies the four startup rules of ADR-034 decision 3 to a
// template's metadata.name. An empty format is legal (the fallback name); a
// non-empty one must stay inside the character-level closure, carry a unique
// task segment, fit the length limit under the worst-case rendering, and
// render DNS-1123 under the minimum-value rendering. Any violation refuses
// startup — a silent rewrite would let a deployer believe a custom format took
// effect.
func validateNameFormat(format string) error {
	if format == "" {
		return nil
	}
	// Character-level closure: every '<...>' literal must be one of the four
	// variables, and everything else must be [a-z0-9-].
	rest := format
	for {
		open := strings.Index(rest, "<")
		if open < 0 {
			if err := validateNameLiteral(rest); err != nil {
				return err
			}
			break
		}
		if err := validateNameLiteral(rest[:open]); err != nil {
			return err
		}
		closeAt := strings.Index(rest[open:], ">")
		if closeAt < 0 {
			return fmt.Errorf("%w: %q must be one of %s/%s/%s/%s", ErrInvalidJobNameFormat,
				rest[open:], nameVarIssueSegment, nameVarAgentSegment, nameVarTaskKey, nameVarTaskID)
		}
		literal := rest[open : open+closeAt+1]
		if _, ok := nameVariableFor(literal); !ok {
			return fmt.Errorf("%w: %q must be one of %s/%s/%s/%s (no other placeholder is legal in metadata.name)",
				ErrInvalidJobNameFormat, literal,
				nameVarIssueSegment, nameVarAgentSegment, nameVarTaskKey, nameVarTaskID)
		}
		rest = rest[open+closeAt+1:]
	}
	if !strings.Contains(format, nameVarTaskKey) && !strings.Contains(format, nameVarTaskID) {
		return fmt.Errorf("%w: %q must carry a unique task segment (%s or %s)",
			ErrInvalidJobNameFormat, format, nameVarTaskKey, nameVarTaskID)
	}
	if worst := renderNameFormat(format, nameWorstValues()); len(worst) > maxJobNameLength {
		return fmt.Errorf("%w: %q renders up to %d characters, over the %d limit",
			ErrInvalidJobNameFormat, format, len(worst), maxJobNameLength)
	}
	if minimal := renderNameFormat(format, nameMinValues()); !dns1123Label.MatchString(minimal) {
		return fmt.Errorf("%w: %q renders %q at its minimum values, which is not DNS-1123",
			ErrInvalidJobNameFormat, format, minimal)
	}
	return nil
}

// validateNameLiteral checks the static part of a name format: lowercase
// letters, digits and dashes only.
func validateNameLiteral(literal string) error {
	for _, r := range literal {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' {
			continue
		}
		return fmt.Errorf("%w: %q is not allowed in metadata.name (lowercase letters, digits, '-' and the name variables only)",
			ErrInvalidJobNameFormat, literal)
	}
	return nil
}

// nameVariableFor returns the variable a literal names.
func nameVariableFor(literal string) (nameVariable, bool) {
	for _, v := range nameVariables {
		if v.literal == literal {
			return v, true
		}
	}
	return nameVariable{}, false
}

// nameValues is the substitution set of one render: the two segment sources
// plus the task id.
type nameValues struct {
	issueSegment string
	agentSegment string
	taskKey      string
	taskID       string
}

// nameWorstValues renders every variable at its longest budget.
func nameWorstValues() nameValues {
	return nameValues{
		issueSegment: strings.Repeat("a", budgetIssueSegment),
		agentSegment: strings.Repeat("a", budgetAgentSegment),
		taskKey:      strings.Repeat("a", budgetTaskKey),
		taskID:       strings.Repeat("a", budgetTaskID),
	}
}

// nameMinValues renders every variable at its shortest possible value.
func nameMinValues() nameValues {
	return nameValues{issueSegment: "a", agentSegment: "a", taskKey: "a", taskID: "a"}
}

// renderNameFormat substitutes the name variables of one format string.
func renderNameFormat(format string, v nameValues) string {
	out := format
	for _, pair := range []struct{ literal, value string }{
		{nameVarIssueSegment, v.issueSegment},
		{nameVarAgentSegment, v.agentSegment},
		{nameVarTaskKey, v.taskKey},
		{nameVarTaskID, v.taskID},
	} {
		out = strings.ReplaceAll(out, pair.literal, pair.value)
	}
	return out
}

// resolveNameValues normalizes a task's fields into the name variables
// (ADR-034 decision 4). The issue segment's only source is the payload's
// issue_identifier: an absent issue context (or an empty identifier) normalizes
// to the fallback, and issue_id is never a substitute — it is a UUID, not a
// human-readable segment, and the design pins the source to the identifier.
func resolveNameValues(e TaskEntry) nameValues {
	return nameValues{
		issueSegment: normalizeSegment(e.IssueIdentifier, budgetIssueSegment, fallbackIssueSegment),
		agentSegment: normalizeSegment(e.AgentName, budgetAgentSegment, fallbackAgentSegment),
		taskKey:      taskKeyOf(e.TaskID),
		taskID:       e.TaskID,
	}
}

// normalizeSegment applies the single normalization rule of the name face:
// lowercase, collapse every run of non-[a-z0-9] into one '-', trim leading and
// trailing '-', truncate to the budget, trim a trailing '-' again, and fall
// back when nothing is left. The result is always DNS-1123-safe on its own.
func normalizeSegment(raw string, budget int, fallback string) string {
	var out []byte
	pendingDash := false
	for i := range len(raw) {
		c := raw[i]
		switch {
		case c >= 'a' && c <= 'z' || c >= '0' && c <= '9':
			if pendingDash && len(out) > 0 {
				out = append(out, '-')
			}
			pendingDash = false
			out = append(out, c)
		case c >= 'A' && c <= 'Z':
			if pendingDash && len(out) > 0 {
				out = append(out, '-')
			}
			pendingDash = false
			out = append(out, c+('a'-'A'))
		default:
			// Any other byte (punctuation, whitespace, a multi-byte rune's
			// continuation byte) collapses into the same separator; a rune
			// therefore never survives normalization, which is exactly the
			// documented loss for non-ASCII names.
			pendingDash = true
		}
	}
	if len(out) > budget {
		out = out[:budget]
	}
	// Truncation can leave a trailing separator; trimming before the cut is
	// not enough (the cut itself can expose one).
	for len(out) > 0 && out[len(out)-1] == '-' {
		out = out[:len(out)-1]
	}
	if len(out) == 0 {
		return fallback
	}
	return string(out)
}

// taskKeyOf is the unique task segment: the last 12 characters of the task id
// with its dashes removed (48 bits of a UUID's hex).
func taskKeyOf(taskID string) string {
	compact := strings.ReplaceAll(taskID, "-", "")
	if len(compact) > budgetTaskKey {
		compact = compact[len(compact)-budgetTaskKey:]
	}
	return compact
}

// ResolveName composes the display name of one task (ADR-034 decision 1): the
// template's metadata.name read as a format string, or the identity name
// fm-<task_id> when the template writes none. The format was validated at
// startup; this is the render-time final check, and a name that leaves
// DNS-1123 or the length limit fails the task as invalid_task_id instead of
// being renamed behind the deployer's back.
func (b *Builder) ResolveName(e TaskEntry) (string, error) {
	format := templateNameFormat(b.template)
	name := fallbackJobName(e.TaskID)
	if format != "" {
		name = renderNameFormat(format, resolveNameValues(e))
	}
	if len(name) > maxJobNameLength || !dns1123Label.MatchString(name) {
		return "", fmt.Errorf("%w: %q does not yield a valid Job name %q", ErrInvalidTaskID, e.TaskID, name)
	}
	return name, nil
}

// templateNameFormat reads the template's metadata.name as the name format
// string; a missing or non-string value reads as "template writes none".
func templateNameFormat(template map[string]any) string {
	metadata, _ := template["metadata"].(map[string]any)
	name, _ := metadata["name"].(string)
	return name
}

// daemonIDFor is the identity name: fm-<task_id>, never the display name
// (ADR-034 decision 8). The daemon-id label, the daemon-id annotation, the
// credential Secret name and the Job Token's j claim all derive from it.
func daemonIDFor(taskID string) string {
	return "fm-" + taskID
}

// fallbackJobName is the display name when the template expresses none — equal
// to the identity name, which is what makes the legacy template
// (`metadata.name: fm-<task_id>`) behave exactly as before.
func fallbackJobName(taskID string) string {
	return daemonIDFor(taskID)
}
