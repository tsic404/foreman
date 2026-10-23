// TC-tech-multiws-08 implements AC-88: one deployment-level template renders
// per Job workspace — the workspace placeholder lands in every string value
// (including repeated occurrences, map values and list elements), while a
// fixed literal survives untouched.
package acceptance

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
)

func TestTechMultiWS08TemplateRendersPerWorkspace(t *testing.T) {
	m := newMultiWS(t, "ws-a", "ws-b")
	builder := newRealMultiBuilder(t, m, []string{"ws-a", "ws-b"}, "")

	jobA, secretA, _, err := builder.Build(taskEntryFor("11111111-1111-4111-8111-111111111111", "ws-a", ""))
	if err != nil {
		t.Fatalf("Build(A): %v", err)
	}
	jobB, secretB, _, err := builder.Build(taskEntryFor("22222222-2222-4222-8222-222222222222", "ws-b", ""))
	if err != nil {
		t.Fatalf("Build(B): %v", err)
	}

	for name, job := range map[string]struct {
		workspaceID string
		notWant     string
	}{"ws-a": {"ws-a", "ws-b"}, "ws-b": {"ws-b", "ws-a"}} {
		ws := job.workspaceID
		built := jobA
		builtSecret := secretA
		if name == "ws-b" {
			built = jobB
			builtSecret = secretB
		}
		if got := built.Labels[registry.LabelWorkspaceID]; got != ws {
			t.Errorf("%s Job workspace label = %q, want %s", name, got, ws)
		}
		if got := built.Spec.Template.Labels[registry.LabelWorkspaceID]; got != ws {
			t.Errorf("%s pod workspace label = %q, want %s", name, got, ws)
		}
		if got := builtSecret.Labels[registry.LabelWorkspaceID]; got != ws {
			t.Errorf("%s credential Secret workspace label = %q, want %s", name, got, ws)
		}
		// Repeated placeholder in one string: all occurrences land.
		if got := built.Spec.Template.Labels["pair"]; got != ws+"|"+ws {
			t.Errorf("%s repeated-placeholder label = %q, want %q", name, got, ws+"|"+ws)
		}
		// List element.
		if got := built.Spec.Template.Spec.Containers[0].Args[0]; got != "--ws="+ws {
			t.Errorf("%s args = %q, want --ws=%s", name, got, ws)
		}
		// env value with two occurrences.
		var twice, root string
		for _, env := range built.Spec.Template.Spec.Containers[0].Env {
			switch env.Name {
			case "WS_TWICE":
				twice = env.Value
			case "MULTICA_WORKSPACES_ROOT":
				root = env.Value
			}
		}
		if twice != ws+"/"+ws {
			t.Errorf("%s WS_TWICE = %q, want %q", name, twice, ws+"/"+ws)
		}
		if root != "/state/workspaces" {
			t.Errorf("%s template-owned env = %q, want it untouched", name, root)
		}
		// Volume paths: the workspace-partitioned one follows the Job; the
		// fixed literal stays exactly as the template wrote it.
		paths := map[string]string{}
		for _, volume := range built.Spec.Template.Spec.Volumes {
			if volume.HostPath != nil {
				paths[volume.Name] = volume.HostPath.Path
			}
		}
		if paths["home"] != "/state/"+ws {
			t.Errorf("%s state root = %q, want /state/%s", name, paths["home"], ws)
		}
		if paths["fixed"] != "/fixed/state" {
			t.Errorf("%s fixed literal = %q, want /fixed/state (Foreman must not rewrite it)", name, paths["fixed"])
		}

		raw, _ := json.Marshal(built)
		if strings.Contains(string(raw), job.notWant) {
			t.Errorf("%s Job carries the other workspace's value %q", name, job.notWant)
		}
	}

	if secretA.StringData["config.json"] == secretB.StringData["config.json"] {
		t.Error("both workspaces share one credential payload")
	}

	// The startup / registration-discovery probe takes the workspace of the
	// credential it holds.
	probe, _, _, err := builder.BuildCatalogProbe("req-8", "runtime-8", "ws-a", time.Minute)
	if err != nil {
		t.Fatalf("BuildCatalogProbe: %v", err)
	}
	if got := probe.Labels[registry.LabelWorkspaceID]; got != "ws-a" {
		t.Errorf("probe workspace label = %q, want ws-a", got)
	}
	probePaths := map[string]string{}
	for _, volume := range probe.Spec.Template.Spec.Volumes {
		if volume.HostPath != nil {
			probePaths[volume.Name] = volume.HostPath.Path
		}
	}
	if probePaths["home"] != "/state/ws-a" {
		t.Errorf("probe state root = %q, want /state/ws-a", probePaths["home"])
	}
	if probePaths["fixed"] != "/fixed/state" {
		t.Errorf("probe fixed literal = %q, want /fixed/state", probePaths["fixed"])
	}
	var probeTwice string
	for _, env := range probe.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "WS_TWICE" {
			probeTwice = env.Value
		}
	}
	if probeTwice != "ws-a/ws-a" {
		t.Errorf("probe WS_TWICE = %q, want ws-a/ws-a", probeTwice)
	}
}
