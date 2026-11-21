package scheduler

import "testing"

// TestProbeScopeStaysOnItsSource pins the probe's admission and discovery
// sets: a source-scoped probe takes its token workspace from that source and
// never falls back to another source's workspace. A source with no configured
// workspace fails closed instead of borrowing one.
func TestProbeScopeStaysOnItsSource(t *testing.T) {
	s := &Scheduler{cfg: Config{
		WorkspaceID: "ws-default",
		Workspaces:  []string{"ws-a", "ws-b"},
		SourceWorkspaces: map[string][]string{
			"src-a": {"ws-a"},
			"src-b": {"ws-b"},
		},
	}}

	if got := s.probeTokenWorkspaceInSource("src-a"); got != "ws-a" {
		t.Errorf("source A probe workspace = %q, want ws-a", got)
	}
	if got := s.probeTokenWorkspaceInSource("src-b"); got != "ws-b" {
		t.Errorf("source B probe workspace = %q, want ws-b", got)
	}
	if got := s.probeTokenWorkspaceInSource("src-missing"); got != "" {
		t.Errorf("unknown source probe workspace = %q, want the empty fail-closed value", got)
	}
	if got := s.probeWorkspaceIDs("src-a"); len(got) != 1 || got[0] != "ws-a" {
		t.Errorf("source A discovery set = %v, want [ws-a]", got)
	}
	if got := s.probeWorkspaceIDs(""); len(got) != 2 {
		t.Errorf("legacy discovery set = %v, want both configured workspaces", got)
	}
}
