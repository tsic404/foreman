package jobbuilder

import (
	"errors"
	"sort"
	"strings"
	"testing"
)

// partitionKeysPolicy is the partition segment fixture: one partition keyed by
// a workspace identifier and one by a display name, each with a distinct
// resource definition so the resolved table is observable.
const partitionKeysPolicy = `
workspaces:
  ws-a:
    agents:
      omp:
        resourcePolicy: agent
        resources:
          requests: {cpu: 100m}
  Team B:
    agents:
      omp:
        resourcePolicy: agent
        resources:
          requests: {cpu: 200m}
`

func partitionKeyRefs() []WorkspaceRef {
	return []WorkspaceRef{
		{ID: "ws-a", Name: "Alpha"},
		{ID: "ws-b", Name: "Team B"},
	}
}

// TestPartitionKeysResolveIdentifiersFirstThenDisplayNames pins §8.5 分区键的
// 启动期解析: the identifier form keeps working, the display-name form resolves
// to the same workspace id, and each resolved partition answers only its own
// workspace. The event keeps the key literal and carries the resolved id.
func TestPartitionKeysResolveIdentifiersFirstThenDisplayNames(t *testing.T) {
	cfg, logs := policyConfig(t, partitionKeysPolicy)
	cfg.Workspaces = partitionKeyRefs()
	policy, err := LoadPolicy(cfg)
	if err != nil {
		t.Fatalf("LoadPolicy: %v", err)
	}

	for _, tc := range []struct{ workspaceID, wantCPU string }{
		{"ws-a", "100m"},
		{"ws-b", "200m"},
	} {
		ev := policy.Evaluate(TaskEntry{WorkspaceID: tc.workspaceID, AgentName: "omp"})
		if ev.resourcePolicy != AxisAgent {
			t.Fatalf("%s resource tier = %s, want agent", tc.workspaceID, ev.resourcePolicy)
		}
		if ev.resources == nil {
			t.Fatalf("%s resolved no resource definition", tc.workspaceID)
		}
		if got := ev.resources.Requests.Cpu().String(); got != tc.wantCPU {
			t.Errorf("%s cpu request = %q, want %q", tc.workspaceID, got, tc.wantCPU)
		}
	}

	// A partition resolved by display name must not answer another workspace,
	// and the mapping is keyed by the workspace identifier, not by the literal.
	if ids := policyWorkspaceIDsOf(policy); strings.Join(ids, ",") != "ws-a,ws-b" {
		t.Errorf("partition ids = %v, want [ws-a ws-b]", ids)
	}
	log := logs.String()
	if !strings.Contains(log, `"key":"Team B"`) {
		t.Errorf("the display-name key literal is missing from the event:\n%s", log)
	}
	if !strings.Contains(log, `"key":"Team B","conclusion":"resolved","resolved_workspace_id":"ws-b"`) {
		t.Errorf("the resolved display-name key does not carry its workspace id:\n%s", log)
	}
}

// TestPartitionKeyAbsentRefusesStartup: a key that is neither a discovered
// identifier nor a discovered display name is a configuration error and names
// the key (§8.5 failure class ⑥).
func TestPartitionKeyAbsentRefusesStartup(t *testing.T) {
	const manifest = `
workspaces:
  ghost:
    agents: {}
`
	cfg, logs := policyConfig(t, manifest)
	cfg.Workspaces = partitionKeyRefs()
	_, err := LoadPolicy(cfg)
	if err == nil || !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("LoadPolicy = %v, want an ErrInvalidPolicy refusal", err)
	}
	if !strings.Contains(err.Error(), `"ghost"`) {
		t.Errorf("the refusal does not name the key: %v", err)
	}
	if !strings.Contains(logs.String(), `"key":"ghost","conclusion":"absent"`) {
		t.Errorf("the absent conclusion is not recorded:\n%s", logs.String())
	}
}

// TestPartitionKeyAmbiguityIsPerKey: duplicate display names in the discovery
// set are only a problem for a key that uses that name. Identifier-only keys
// boot; the name key refuses and names itself (§8.5 failure class ⑦).
func TestPartitionKeyAmbiguityIsPerKey(t *testing.T) {
	refs := []WorkspaceRef{
		{ID: "ws-a", Name: "dup"},
		{ID: "ws-b", Name: "dup"},
	}

	t.Run("duplicate names with identifier keys boot", func(t *testing.T) {
		const manifest = `
workspaces:
  ws-a:
    agents: {}
  ws-b:
    agents: {}
`
		cfg, _ := policyConfig(t, manifest)
		cfg.Workspaces = refs
		if _, err := LoadPolicy(cfg); err != nil {
			t.Fatalf("LoadPolicy refused identifier keys over a duplicate-name discovery set: %v", err)
		}
	})

	t.Run("the key that writes the duplicated name refuses", func(t *testing.T) {
		const manifest = `
workspaces:
  dup:
    agents: {}
  ws-b:
    agents: {}
`
		cfg, logs := policyConfig(t, manifest)
		cfg.Workspaces = refs
		_, err := LoadPolicy(cfg)
		if err == nil || !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("LoadPolicy = %v, want an ErrInvalidPolicy refusal", err)
		}
		if !strings.Contains(err.Error(), `"dup"`) {
			t.Errorf("the refusal does not name the ambiguous key: %v", err)
		}
		if !strings.Contains(logs.String(), `"key":"dup","conclusion":"ambiguous"`) {
			t.Errorf("the ambiguous conclusion is not recorded:\n%s", logs.String())
		}
		if strings.Contains(logs.String(), `"key":"ws-b","conclusion":"ambiguous"`) {
			t.Errorf("the identifier key was judged ambiguous:\n%s", logs.String())
		}
	})
}

// TestPartitionKeysOnOneWorkspaceRefuse: identifier and display name pointing
// at the same workspace leave its definitions with two rows. Both keys refuse,
// and each records its own conflict warn carrying the shared workspace id —
// no third event (§8.5 failure class ⑧, §9.1).
func TestPartitionKeysOnOneWorkspaceRefuse(t *testing.T) {
	const manifest = `
workspaces:
  ws-a:
    agents: {}
  Alpha:
    agents: {}
`
	cfg, logs := policyConfig(t, manifest)
	cfg.Workspaces = partitionKeyRefs()
	_, err := LoadPolicy(cfg)
	if err == nil || !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("LoadPolicy = %v, want an ErrInvalidPolicy refusal", err)
	}
	if !strings.Contains(err.Error(), `"ws-a"`) || !strings.Contains(err.Error(), `"Alpha"`) {
		t.Errorf("the refusal does not name both keys: %v", err)
	}
	log := logs.String()
	if got := strings.Count(log, `"conclusion":"conflict"`); got != 2 {
		t.Errorf("conflict events = %d, want two (one per key):\n%s", got, log)
	}
	if got := strings.Count(log, `"resolved_workspace_id":"ws-a"`); got != 2 {
		t.Errorf("conflict events carrying the workspace id = %d, want two:\n%s", got, log)
	}
	if !strings.Contains(log, `"level":"WARN"`) {
		t.Errorf("the conflict conclusion must be warn level:\n%s", log)
	}
}

// TestPartitionResolutionIsStartupOnly: the mapping freezes at load, so a
// display-name change after startup cannot re-resolve (the old literal keeps
// pointing at the same workspace id); on the next boot the old name is absent
// and refuses.
func TestPartitionResolutionIsStartupOnly(t *testing.T) {
	cfg, _ := policyConfig(t, partitionKeysPolicy)
	refs := partitionKeyRefs()
	cfg.Workspaces = refs
	builder := newTestBuilder(t, cfg)

	// The runtime never consults the discovery set again. The refs slice is
	// shared by value (same backing array), so mutating the name here would
	// change the set any re-resolving implementation reads — the frozen table
	// must still answer ws-b's partition by id.
	refs[1].Name = "Renamed"
	ev := builder.policy.Evaluate(TaskEntry{WorkspaceID: "ws-b", AgentName: "omp"})
	if ev.resources == nil || ev.resources.Requests.Cpu().String() != "200m" {
		t.Errorf("a post-startup display-name change re-resolved the partition: %+v", ev.resources)
	}
	if have := len(builder.policy.byWorkspace); have != 2 {
		t.Errorf("the loaded policy re-resolved its partitions: have %d tables, want 2 (frozen at startup)", have)
	}

	// The next boot sees the new display name, so the old literal is absent.
	cfg.Workspaces = []WorkspaceRef{{ID: "ws-a", Name: "Alpha"}, {ID: "ws-b", Name: "Renamed"}}
	if _, err := LoadPolicy(cfg); err == nil {
		t.Error("a restart with the old display name must refuse startup")
	}
}

// TestPartitionKeyUnseededAuthority: a display name whose authority has not
// answered yet (workspace-scoped form, transient C-17 failure) is not judged
// absent — the key warns as unseeded and the boot continues. On the next boot
// with the name seeded the same key resolves (§1.1, AC-86).
func TestPartitionKeyUnseededAuthority(t *testing.T) {
	const manifest = `
workspaces:
  Team B:
    agents: {}
`
	t.Run("unmatched name key over an unseeded set warns and continues", func(t *testing.T) {
		cfg, logs := policyConfig(t, manifest)
		cfg.Workspaces = []WorkspaceRef{{ID: "ws-a"}}
		policy, err := LoadPolicy(cfg)
		if err != nil {
			t.Fatalf("LoadPolicy refused a name key over an unseeded authority: %v", err)
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
		// The partition stays unreadable this process — never a definition.
		ev := policy.Evaluate(TaskEntry{WorkspaceID: "ws-a", AgentName: "omp"})
		if ev.resourcePolicy != AxisNone {
			t.Errorf("unseeded partition tier = %s, want none", ev.resourcePolicy)
		}
		if ev.resources != nil {
			t.Errorf("an unseeded partition must not inject resources, got %+v", ev.resources)
		}
	})

	t.Run("identifier keys are unaffected by an unseeded authority", func(t *testing.T) {
		const identifierKeys = `
workspaces:
  ws-a:
    agents:
      omp:
        resourcePolicy: agent
        resources:
          requests: {cpu: 100m}
  Team B:
    agents: {}
`
		cfg, logs := policyConfig(t, identifierKeys)
		cfg.Workspaces = []WorkspaceRef{{ID: "ws-a"}}
		policy, err := LoadPolicy(cfg)
		if err != nil {
			t.Fatalf("LoadPolicy refused identifier keys over an unseeded authority: %v", err)
		}
		if ids := policyWorkspaceIDsOf(policy); strings.Join(ids, ",") != "ws-a" {
			t.Errorf("resolved partition ids = %v, want [ws-a]", ids)
		}
		// The identifier key resolves before the seeding judgement: its
		// partition answers ws-a even though the name side is unseeded.
		ev := policy.Evaluate(TaskEntry{WorkspaceID: "ws-a", AgentName: "omp"})
		if ev.resourcePolicy != AxisAgent {
			t.Errorf("identifier-key partition tier = %s, want agent", ev.resourcePolicy)
		}
		if ev.resources == nil || ev.resources.Requests.Cpu().String() != "100m" {
			t.Errorf("identifier-key partition injected %+v, want cpu 100m", ev.resources)
		}
		log := logs.String()
		if !strings.Contains(log, `"key":"ws-a","conclusion":"resolved","resolved_workspace_id":"ws-a"`) {
			t.Errorf("the identifier key's resolution is not recorded:\n%s", log)
		}
		if !strings.Contains(log, `"key":"Team B","conclusion":"unseeded"`) {
			t.Errorf("the name key's unseeded conclusion is not recorded:\n%s", log)
		}
	})

	t.Run("seeded set still refuses a truly absent key", func(t *testing.T) {
		cfg, _ := policyConfig(t, manifest)
		cfg.Workspaces = []WorkspaceRef{{ID: "ws-a", Name: "Other"}}
		if _, err := LoadPolicy(cfg); err == nil || !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("LoadPolicy = %v, want a refusal over a fully seeded set", err)
		}
	})

	t.Run("the key resolves on the next boot once the name is seeded", func(t *testing.T) {
		cfg, _ := policyConfig(t, manifest)
		cfg.Workspaces = []WorkspaceRef{{ID: "ws-a", Name: "Team B"}}
		policy, err := LoadPolicy(cfg)
		if err != nil {
			t.Fatalf("LoadPolicy over the seeded authority: %v", err)
		}
		ev := policy.Evaluate(TaskEntry{WorkspaceID: "ws-a", AgentName: "omp"})
		if ev.resourcePolicy != AxisNone {
			t.Errorf("tier = %s, want none (an agents:{} partition defines nothing)", ev.resourcePolicy)
		}
		if ids := policyWorkspaceIDsOf(policy); strings.Join(ids, ",") != "ws-a" {
			t.Errorf("resolved partition ids = %v, want [ws-a]", ids)
		}
	})
}

// TestWarnDeadPartitions: a resolved partition outside the effective service
// scope is dead configuration — one warn per partition, never an error
// (AC-44 / AC-98).
func TestWarnDeadPartitions(t *testing.T) {
	cfg, logs := policyConfig(t, partitionKeysPolicy)
	cfg.Workspaces = partitionKeyRefs()
	builder := newTestBuilder(t, cfg)

	builder.WarnDeadPartitions([]string{"ws-a"})
	if got := strings.Count(logs.String(), "outside the effective service scope"); got != 1 {
		t.Fatalf("dead-partition warns = %d, want one for ws-b:\n%s", got, logs.String())
	}
	if !strings.Contains(logs.String(), `"workspace_id":"ws-b"`) {
		t.Errorf("the dead partition is not named:\n%s", logs.String())
	}

	logs.Reset()
	builder.WarnDeadPartitions([]string{"ws-a", "ws-b"})
	if strings.Contains(logs.String(), "outside the effective service scope") {
		t.Errorf("a fully served policy warned about dead partitions:\n%s", logs.String())
	}
}

// policyWorkspaceIDsOf lists a Policy's resolved partition ids in order, the
// way the builder exposes them to the service-scope check.
func policyWorkspaceIDsOf(policy Policy) []string {
	ids := make([]string, 0, len(policy.byWorkspace))
	for id := range policy.byWorkspace {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
