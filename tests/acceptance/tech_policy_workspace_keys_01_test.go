// TC-tech-policy-workspace-keys-01 implements AC-98: the policy file's
// partition keys are workspace identifiers or display names, resolved once at
// startup against the discovery result (identifier exact match first, display
// name second). A key that names nothing, a key whose display name matches
// more than one discovered workspace, and two keys landing on one workspace
// all refuse startup and name the key; duplicate display names with
// identifier-only keys boot normally. A resolved partition outside the
// effective service scope is dead configuration — warn only. The mapping
// freezes at startup: a display-name change mid-process does not re-resolve,
// while a restart with the old name refuses.
package acceptance

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/jobbuilder"
)

// tcWorkspaceKeysPolicy is the partition fixture: one identifier key and one
// display-name key with distinct resource definitions, so the resolved
// partition is observable on the rendered Job.
const tcWorkspaceKeysPolicy = `
workspaces:
  ws-a:
    agents:
      shared:
        resourcePolicy: agent
        resources:
          requests: {cpu: 100m}
  Team B:
    agents:
      shared:
        resourcePolicy: agent
        resources:
          requests: {cpu: 200m}
`

// tcWorkspaceKeysRefs is the discovery result the fixture resolves against.
func tcWorkspaceKeysRefs() []jobbuilder.WorkspaceRef {
	return []jobbuilder.WorkspaceRef{
		{ID: "ws-a", Name: "Alpha"},
		{ID: "ws-b", Name: "Team B"},
	}
}

// tcWorkspaceKeysWrite writes one policy file inside the test's workdir.
func tcWorkspaceKeysWrite(t *testing.T, manifest string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(path, []byte(manifest), 0o600); err != nil {
		t.Fatalf("write policy: %v", err)
	}
	return path
}

// tcWorkspaceKeysConfig builds the module configuration the startup gate runs
// with: the reference template, the policy carrier and the discovery refs.
func tcWorkspaceKeysConfig(t *testing.T, manifest string, refs []jobbuilder.WorkspaceRef) (jobbuilder.Config, *bytes.Buffer) {
	t.Helper()
	path := tcWorkspaceKeysWrite(t, manifest)
	return tcPolicyAxisConfig(t, deployedTemplatePath(t), path, func(cfg *jobbuilder.Config) {
		cfg.Workspaces = refs
		ids := make([]string, 0, len(refs))
		for _, ref := range refs {
			ids = append(ids, ref.ID)
		}
		cfg.WorkspaceIDs = ids
	})
}

// tcWorkspaceKeysBuilder runs the production startup gate and fails the case
// if the boot is refused.
func tcWorkspaceKeysBuilder(t *testing.T, manifest string, refs []jobbuilder.WorkspaceRef) (*jobbuilder.Builder, *bytes.Buffer) {
	t.Helper()
	cfg, logs := tcWorkspaceKeysConfig(t, manifest, refs)
	builder, err := jobbuilder.NewBuilder(cfg)
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	return builder, logs
}

// tcWorkspaceKeysRefusal runs the startup gate expecting a refusal and returns
// the error and the log it wrote.
func tcWorkspaceKeysRefusal(t *testing.T, manifest string, refs []jobbuilder.WorkspaceRef) (error, string) {
	t.Helper()
	cfg, logs := tcWorkspaceKeysConfig(t, manifest, refs)
	_, err := jobbuilder.NewBuilder(cfg)
	if err == nil {
		t.Fatal("NewBuilder accepted the policy, want a refusal")
	}
	if !errors.Is(err, jobbuilder.ErrInvalidPolicy) {
		t.Fatalf("error = %v, want ErrInvalidPolicy", err)
	}
	return err, logs.String()
}

// tcWorkspaceKeysCPU renders one task in a workspace and returns its first
// container's CPU request.
func tcWorkspaceKeysCPU(t *testing.T, builder *jobbuilder.Builder, taskID, workspaceID string) string {
	t.Helper()
	job, _, _, err := builder.Build(taskEntryFor(taskID, workspaceID, "shared"))
	if err != nil {
		t.Fatalf("Build(%s): %v", workspaceID, err)
	}
	return job.Spec.Template.Spec.Containers[0].Resources.Requests.Cpu().String()
}

// TestPolicyWorkspaceKeys01 is AC-98's executable form.
func TestPolicyWorkspaceKeys01(t *testing.T) {
	t.Run("identifier and display-name keys inject per workspace", func(t *testing.T) {
		builder, logs := tcWorkspaceKeysBuilder(t, tcWorkspaceKeysPolicy, tcWorkspaceKeysRefs())

		if got := tcWorkspaceKeysCPU(t, builder, "11111111-1111-4111-8111-111111111111", "ws-a"); got != "100m" {
			t.Errorf("ws-a cpu request = %q, want 100m", got)
		}
		// The second partition is keyed by the display name "Team B"; its
		// definitions answer only the workspace it resolved to.
		if got := tcWorkspaceKeysCPU(t, builder, "22222222-2222-4222-8222-222222222222", "ws-b"); got != "200m" {
			t.Errorf("ws-b cpu request = %q, want 200m (the display-name partition did not resolve)", got)
		}

		log := logs.String()
		for _, want := range []string{
			`"kind":"workspace"`,
			`"key":"ws-a","conclusion":"resolved","resolved_workspace_id":"ws-a"`,
			`"key":"Team B","conclusion":"resolved","resolved_workspace_id":"ws-b"`,
		} {
			if !strings.Contains(log, want) {
				t.Errorf("partition event missing %s:\n%s", want, log)
			}
		}
	})

	t.Run("a key that names nothing refuses and names the key", func(t *testing.T) {
		const manifest = `
workspaces:
  ghost:
    agents: {}
`
		err, log := tcWorkspaceKeysRefusal(t, manifest, tcWorkspaceKeysRefs())
		if !strings.Contains(err.Error(), `"ghost"`) {
			t.Errorf("the refusal does not name the key: %v", err)
		}
		if !strings.Contains(log, `"key":"ghost","conclusion":"absent"`) {
			t.Errorf("the absent conclusion is not recorded:\n%s", log)
		}
	})

	t.Run("duplicate display names: identifier keys boot, the name key refuses", func(t *testing.T) {
		refs := []jobbuilder.WorkspaceRef{
			{ID: "ws-a", Name: "dup"},
			{ID: "ws-b", Name: "dup"},
		}
		const identifierKeys = `
workspaces:
  ws-a:
    agents: {}
  ws-b:
    agents: {}
`
		if _, err := jobbuilder.NewBuilder(mustWorkspaceKeysConfig(t, identifierKeys, refs)); err != nil {
			t.Fatalf("identifier-only keys over a duplicate-name set refused startup: %v", err)
		}

		const nameKey = `
workspaces:
  dup:
    agents: {}
  ws-b:
    agents: {}
`
		err, log := tcWorkspaceKeysRefusal(t, nameKey, refs)
		if !strings.Contains(err.Error(), `"dup"`) {
			t.Errorf("the refusal does not name the ambiguous key: %v", err)
		}
		if !strings.Contains(log, `"key":"dup","conclusion":"ambiguous"`) {
			t.Errorf("the ambiguous conclusion is not recorded:\n%s", log)
		}
		if strings.Contains(log, `"key":"ws-b","conclusion":"ambiguous"`) {
			t.Errorf("the identifier key was judged ambiguous:\n%s", log)
		}
	})

	t.Run("two keys on one workspace refuse with one warn per key", func(t *testing.T) {
		const manifest = `
workspaces:
  ws-a:
    agents: {}
  Alpha:
    agents: {}
`
		err, log := tcWorkspaceKeysRefusal(t, manifest, tcWorkspaceKeysRefs())
		if !strings.Contains(err.Error(), `"ws-a"`) || !strings.Contains(err.Error(), `"Alpha"`) {
			t.Errorf("the refusal does not name both keys: %v", err)
		}
		if got := strings.Count(log, `"conclusion":"conflict"`); got != 2 {
			t.Errorf("conflict events = %d, want two (one per key):\n%s", got, log)
		}
		if got := strings.Count(log, `"resolved_workspace_id":"ws-a"`); got != 2 {
			t.Errorf("conflict events carrying the workspace id = %d, want two:\n%s", got, log)
		}
		if !strings.Contains(log, `"level":"WARN"`) {
			t.Errorf("the conflict conclusion must be warn level:\n%s", log)
		}
	})

	t.Run("a partition outside the effective service scope only warns", func(t *testing.T) {
		builder, logs := tcWorkspaceKeysBuilder(t, tcWorkspaceKeysPolicy, tcWorkspaceKeysRefs())
		// The startup probe registered ws-a only: ws-b's partition is dead
		// configuration and must not turn into a startup error.
		builder.WarnDeadPartitions([]string{"ws-a"})
		log := logs.String()
		if got := strings.Count(log, "outside the effective service scope"); got != 1 {
			t.Fatalf("dead-partition warns = %d, want one for ws-b:\n%s", got, log)
		}
		if !strings.Contains(log, `"workspace_id":"ws-b"`) {
			t.Errorf("the dead partition is not named:\n%s", log)
		}

		logs.Reset()
		builder.WarnDeadPartitions([]string{"ws-a", "ws-b"})
		if strings.Contains(logs.String(), "outside the effective service scope") {
			t.Errorf("a fully served policy warned about dead partitions:\n%s", logs.String())
		}
	})

	t.Run("a runtime display-name change does not re-resolve; the old name refuses on restart", func(t *testing.T) {
		refs := tcWorkspaceKeysRefs()
		builder, logs := tcWorkspaceKeysBuilder(t, tcWorkspaceKeysPolicy, refs)
		before := strings.Count(logs.String(), `"kind":"workspace"`)

		// The discovery set's display name changes after startup (in-process,
		// the same backing slice the load saw).
		refs[1].Name = "Renamed"

		// The loaded policy still routes ws-b by identifier: no re-resolution,
		// no reliance on the display name at runtime.
		if got := tcWorkspaceKeysCPU(t, builder, "33333333-3333-4333-8333-333333333333", "ws-b"); got != "200m" {
			t.Errorf("after the rename, ws-b cpu request = %q, want 200m", got)
		}
		if got := strings.Count(logs.String(), `"kind":"workspace"`); got != before {
			t.Errorf("partition events grew from %d to %d after runtime evaluation:\n%s", before, got, logs.String())
		}

		// The next boot sees "Team B" only under its new name; the policy's
		// old literal is absent and refuses.
		err, _ := tcWorkspaceKeysRefusal(t, tcWorkspaceKeysPolicy, []jobbuilder.WorkspaceRef{
			{ID: "ws-a", Name: "Alpha"},
			{ID: "ws-b", Name: "Renamed"},
		})
		if !strings.Contains(err.Error(), `"Team B"`) {
			t.Errorf("the restart refusal does not name the stale key: %v", err)
		}
	})

	t.Run("workspace-scoped C-17 transient failure leaves name keys unseeded, never absent", func(t *testing.T) {
		refs := []jobbuilder.WorkspaceRef{
			{ID: "ws-a", Name: "Alpha"},
			{ID: "ws-b"}, // the self-check's C-17 never answered (AC-86)
		}
		const nameKeyPolicy = `
workspaces:
  ws-a:
    agents:
      shared:
        resourcePolicy: agent
        resources:
          requests: {cpu: 300m}
  Team B:
    agents:
      shared:
        resourcePolicy: agent
        resources:
          requests: {cpu: 400m}
`

		// The name key over the unseeded authority warns and boots; it is not
		// judged absent, and the unseeded partition never enters the runtime
		// as a definition.
		cfg, logs := tcWorkspaceKeysConfig(t, nameKeyPolicy, refs)
		builder, err := jobbuilder.NewBuilder(cfg)
		if err != nil {
			t.Fatalf("a name-key policy over a transiently unseeded authority refused startup: %v", err)
		}
		log := logs.String()
		if !strings.Contains(log, `"key":"Team B","conclusion":"unseeded"`) {
			t.Errorf("the unseeded conclusion is not recorded:\n%s", log)
		}
		if !strings.Contains(log, `"level":"WARN"`) {
			t.Errorf("the unseeded conclusion must be warn level:\n%s", log)
		}
		if strings.Contains(log, `"resolved_workspace_id":"ws-b"`) {
			t.Errorf("the unseeded event must not carry a resolved workspace id:\n%s", log)
		}
		// The unseeded partition is not injected into the runtime: ws-b's
		// tasks render the template defaults, not the Team B section.
		if got := tcWorkspaceKeysCPU(t, builder, "33333333-3333-4333-8333-333333333333", "ws-b"); got != "1" {
			t.Errorf("ws-b cpu request = %q, want the template default 1 (unseeded partition read)", got)
		}
		// hasID outranks the seeding judgement: the identifier key still
		// resolves while the name authority is unseeded.
		if got := tcWorkspaceKeysCPU(t, builder, "44444444-4444-4444-8444-444444444444", "ws-a"); got != "300m" {
			t.Errorf("ws-a cpu request = %q, want 300m (identifier key caught by unseeded)", got)
		}

		// With the name seeded (C-17 recovered), the next boot resolves it.
		refs[1].Name = "Team B"
		cfg, _ = tcWorkspaceKeysConfig(t, nameKeyPolicy, refs)
		builder, err = jobbuilder.NewBuilder(cfg)
		if err != nil {
			t.Fatalf("a name-key policy over a recovered authority refused startup: %v", err)
		}
		if got := tcWorkspaceKeysCPU(t, builder, "55555555-5555-4555-8555-555555555555", "ws-b"); got != "400m" {
			t.Errorf("on the recovered boot ws-b cpu request = %q, want 400m (the name key did not resolve)", got)
		}
	})
}

// mustWorkspaceKeysConfig runs the startup gate over a policy/ref pair and
// fails the case on a refusal.
func mustWorkspaceKeysConfig(t *testing.T, manifest string, refs []jobbuilder.WorkspaceRef) jobbuilder.Config {
	t.Helper()
	cfg, _ := tcWorkspaceKeysConfig(t, manifest, refs)
	return cfg
}
