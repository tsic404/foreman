package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/catalog"
	"github.com/tsic404/foreman/internal/discovery"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/release"
)

func TestCatalogAnswerKeepsEmptyAndNonEmptyDistinct(t *testing.T) {
	nonEmpty, err := catalogAnswerFor(&discovery.Result{
		Class: discovery.ClassNonEmpty, Provider: "omp",
		Models: []discovery.Model{{ID: "m1", Provider: "omp", Version: "1"}},
	})
	if err != nil {
		t.Fatalf("non-empty: %v", err)
	}
	var got struct {
		Provider string            `json:"provider"`
		Models   []discovery.Model `json:"models"`
	}
	if err := json.Unmarshal(nonEmpty.Body, &got); err != nil {
		t.Fatalf("non-empty body is not JSON: %v", err)
	}
	if got.Provider != "omp" || len(got.Models) != 1 || got.Models[0].ID != "m1" {
		t.Errorf("non-empty body = %s", nonEmpty.Body)
	}

	empty, err := catalogAnswerFor(&discovery.Result{Class: discovery.ClassEmpty, Provider: "omp"})
	if err != nil {
		t.Fatalf("empty: %v", err)
	}
	var raw struct {
		Models json.RawMessage `json:"models"`
	}
	if err := json.Unmarshal(empty.Body, &raw); err != nil {
		t.Fatalf("empty body is not JSON: %v", err)
	}
	if string(raw.Models) != "[]" {
		t.Errorf("empty body models = %s, want an empty array rather than null", raw.Models)
	}
}

func TestCatalogAnswerDoesNotTurnUnsupportedOrFailedIntoAModelList(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result *discovery.Result
		want   string
	}{
		{"unsupported", &discovery.Result{Class: discovery.ClassUnsupported, Detail: "codex app-server is not verified"}, "unsupported"},
		{"failed", &discovery.Result{Class: discovery.ClassFailed, Detail: "adapter exited 1"}, "failed"},
		{"no result", nil, "without a result"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			answer, err := catalogAnswerFor(tc.result)
			if err == nil {
				t.Fatalf("answer = %s, want an error", answer.Body)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to mention %q", err, tc.want)
			}
			if len(answer.Body) != 0 {
				t.Errorf("body = %s, want no model list", answer.Body)
			}
		})
	}
}

// TestReleaseRunIndexKeepsQueuedRunsAndFrozenFacts: a claimed attempt is in
// flight before its start fact, and the preview reads the claim's frozen
// version/digest/revision rather than deriving them later.
func TestReleaseRunIndexKeepsQueuedRunsAndFrozenFacts(t *testing.T) {
	reg := registry.New(func() time.Time { return time.Now() })
	if err := reg.Put(registry.TaskEntry{
		TaskID: "task-queued", WorkspaceID: "core", AgentID: "agent-1",
		AgentName: "dev", RuntimeIdentity: "omp\x00primary",
		State:      registry.StatePending,
		CLIVersion: "2.0.0", CLIDigests: map[string]string{"linux/amd64": "a"},
		AgentCLIVersion: "3.0.0", AgentCLIDigests: map[string]string{"linux/arm64": "b"},
		ConfigRevision: 7, CLIFactsKnown: true, AgentFactsKnown: true, ConfigFactsKnown: true,
	}); err != nil {
		t.Fatalf("Put queued: %v", err)
	}
	if err := reg.Put(registry.TaskEntry{
		TaskID: "task-terminal", WorkspaceID: "core", AgentID: "agent-2",
		State: registry.StateTerminal, Result: registry.ResultCompleted,
		StartedAt: time.Now(), CLIFactsKnown: true,
	}); err != nil {
		t.Fatalf("Put terminal: %v", err)
	}
	index := releaseRunIndex{
		registry: reg,
		sourceID: func(context.Context, string) (catalog.ServerID, bool) {
			return "atlas", true
		},
	}
	runs := index.Running(context.Background())
	if len(runs) != 1 || runs[0].TaskID != "task-queued" {
		t.Fatalf("runs = %+v, want only the queued attempt", runs)
	}
	run := runs[0]
	if !run.CLIKnown || run.MulticaVersion != "2.0.0" || run.MulticaDigests["linux/amd64"] != "a" {
		t.Fatalf("Multica facts = %+v", run)
	}
	if !run.AgentKnown || run.AgentVersion != "3.0.0" || run.AgentDigests["linux/arm64"] != "b" {
		t.Fatalf("Agent facts = %+v", run)
	}
	if !run.ConfigKnown || run.ConfigRevision != 7 || run.Adapter != "omp" {
		t.Fatalf("config/adapter facts = %+v", run)
	}
}

type staticDirectory struct{}

func (staticDirectory) ReadSource(context.Context, string) (release.SourceDirectory, bool) {
	return release.SourceDirectory{ServerID: "atlas", Synced: true}, true
}

// TestImpactKeepsAnUnknownActualVersionWhenTheManagementPointerMoved: the
// management pointer is v2, but the in-flight attempt has no evidence that it
// executed v2. The preview must list it as an unknown-version old run rather
// than relabel it v2 or omit it.
func TestImpactKeepsAnUnknownActualVersionWhenTheManagementPointerMoved(t *testing.T) {
	reg := registry.New(func() time.Time { return time.Now() })
	if err := reg.Put(registry.TaskEntry{
		TaskID: "task-old-image", WorkspaceID: "core", AgentID: "agent-1",
		AgentName: "dev", RuntimeIdentity: "omp\x00primary",
		State: registry.StatePending,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	store := release.NewMemoryStore()
	service, err := release.New(release.Config{
		Store: store, Directory: staticDirectory{},
		Runs: releaseRunIndex{registry: reg, sourceID: func(context.Context, string) (catalog.ServerID, bool) {
			return "atlas", true
		}},
		TickInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("release.New: %v", err)
	}
	if err := store.PutDefinition(context.Background(), release.Definition{
		ID: "omp", Kind: release.KindAgent, Adapter: "omp", ServerID: "atlas",
		Current: &release.Release{Version: "2.0.0", Generation: 1},
	}); err != nil {
		t.Fatalf("PutDefinition: %v", err)
	}
	impact, err := service.Impact(context.Background(), release.ImpactRequest{CLIID: "omp", Version: "2.0.0"})
	if err != nil {
		t.Fatalf("Impact: %v", err)
	}
	if len(impact.InFlight) != 1 {
		t.Fatalf("in-flight = %+v, want the unknown-version run kept visible", impact.InFlight)
	}
	if impact.InFlight[0].VersionKnown || impact.InFlight[0].Version != "" {
		t.Fatalf("run = %+v, want the management pointer not presented as an executed version", impact.InFlight[0])
	}
}
