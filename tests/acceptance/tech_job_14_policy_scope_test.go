// TC-tech-job-14 implements AC-45's negative scope: the injection face is
// exactly three A1.5 write points on containers[0] and the pod template. The
// init containers and the catalog probe never read the policy, a miss or an
// empty definition leaves the template's Job untouched, and a hit's diff
// against a policy-free render stays inside the three faces (plus the
// nodeSelector hit's soft-affinity suppression, ADR-036).
package acceptance

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/jobbuilder"
)

// tcJob14Policy hits all three axes for one agent, so a scope exclusion is
// judged against a policy that would write every face if it could.
const tcJob14Policy = `
agents:
  omp:
    resourcePolicy: agent
    capPolicy: agent
    nodeSelectorPolicy: agent
    resources:
      requests: { cpu: "3" }
      limits:   { cpu: "6" }
    capabilities:
      add: ["NET_ADMIN", "SYS_ADMIN"]
    nodeSelector: { disktype: ssd }
`

// tcJob14NodeIndex is the soft-affinity seam: one session's node is known and
// never saturated, so the template's <preferred node> item resolves.
type tcJob14NodeIndex struct{}

func (tcJob14NodeIndex) LastNodeForSession(string) string { return "node-a" }
func (tcJob14NodeIndex) Saturated(string) bool            { return false }

// tcJob14Entry is the task both renders of one case share: the same entry is
// what makes their diff attributable to the policy alone.
func tcJob14Entry() jobbuilder.TaskEntry {
	return jobbuilder.TaskEntry{
		TaskID: "task-1", AgentID: "agent-1", WorkspaceID: "ws-1",
		JobRuntimeID: "rid-1", ClaimedAt: testClaimTime, Attempt: 1,
		AgentName: "omp", ProjectID: projectIDFor("proj-1"), ProjectSource: jobbuilder.ProjectSourcePayload,
	}
}

// TestJob14InitContainersAreOutsideTheFace is AC-45's first exclusion: the
// template's init container keeps its resources and securityContext verbatim
// while the same policy writes the first regular container.
func TestJob14InitContainersAreOutsideTheFace(t *testing.T) {
	templatePath := deployedTemplatePath(t)
	plain, _ := tcPolicyAxisBuilder(t, templatePath, "", nil)
	policyPath := writePolicyFile(t, tcJob14Policy)
	withPolicy, _ := tcPolicyAxisBuilder(t, templatePath, policyPath, nil)

	baseline := tcJob14Build(t, plain)
	job := tcJob14Build(t, withPolicy)

	// The policy did write: the first regular container is the control.
	tcPolicyAxisWantResources(t, job.Spec.Template.Spec.Containers[0].Resources,
		map[string]string{"cpu": "3"}, map[string]string{"cpu": "6"})

	baseInit := baseline.Spec.Template.Spec.InitContainers
	init := job.Spec.Template.Spec.InitContainers
	if len(init) != len(baseInit) {
		t.Fatalf("initContainers = %v, want the template's %v", containerNames(init), containerNames(baseInit))
	}
	for i := range init {
		if got, want := tcJob14JSON(t, init[i]), tcJob14JSON(t, baseInit[i]); got != want {
			t.Errorf("initContainers[%d] = %s, want the template's %s", i, got, want)
		}
	}
	// The template's own init values, named (the capability-axis definition
	// must not land here: this container's add list is the template's own).
	// The reference template carries two init containers, so the named face
	// is selected by name rather than by position.
	var prepare *corev1.Container
	for i := range init {
		if init[i].Name == "prepare" {
			prepare = &init[i]
		}
	}
	if prepare == nil {
		t.Fatalf("initContainers = %v, want the template's prepare container", containerNames(init))
	}
	tcPolicyAxisWantResources(t, prepare.Resources,
		map[string]string{"cpu": "20m", "memory": "32Mi"},
		map[string]string{"cpu": "200m", "memory": "128Mi"})
	tcPolicyAxisWantSet(t, "initContainers[prepare] capabilities.drop", tcJob14Capabilities(*prepare, true), "ALL")
	tcPolicyAxisWantSet(t, "initContainers[prepare] capabilities.add", tcJob14Capabilities(*prepare, false),
		"CHOWN", "DAC_OVERRIDE")
}

// TestJob14CatalogProbeIgnoresPolicy is AC-45's second exclusion: the probe Job
// is rendered from the same template copy but through the probe write face, so
// no axis reaches it — its resources, capability add list and nodeSelector are
// identical with and without a policy that hits the task path.
func TestJob14CatalogProbeIgnoresPolicy(t *testing.T) {
	templatePath := deployedTemplatePath(t)
	plain, _ := tcPolicyAxisBuilder(t, templatePath, "", nil)
	policyPath := writePolicyFile(t, tcJob14Policy)
	withPolicy, _ := tcPolicyAxisBuilder(t, templatePath, policyPath, nil)

	probe := func(builder *jobbuilder.Builder) *batchv1.Job {
		t.Helper()
		job, _, _, err := builder.BuildCatalogProbe("req-1", "rid-probe", "ws-1", 30*time.Second)
		if err != nil {
			t.Fatalf("BuildCatalogProbe: %v", err)
		}
		return job
	}
	baseline, got := probe(plain), probe(withPolicy)

	if a, b := tcJob14JSON(t, baseline), tcJob14JSON(t, got); a != b {
		t.Errorf("the probe Job changed with the policy loaded\n--- without ---\n%s\n--- with ---\n%s", a, b)
	}
	tcPolicyAxisWantResources(t, got.Spec.Template.Spec.Containers[0].Resources,
		tcPolicyAxisTemplateRequests, tcPolicyAxisTemplateLimits)
	_, add := tcPolicyAxisCapabilities(got)
	if len(add) != 0 {
		t.Errorf("probe containers[0] capabilities.add = %v, want none", add)
	}
	tcPolicyAxisWantSet(t, "probe containers[0] capabilities.drop", tcPolicyAxisDrop(got), "ALL")
	tcPolicyAxisWantNodeSelector(t, got.Spec.Template.Spec, map[string]string{"kubernetes.io/os": "linux"})
	if got.Name != "fm-catalog-req-1" {
		t.Errorf("probe name = %q, want fm-catalog-req-1", got.Name)
	}
}

// TestJob14MissOrEmptyDefinitionLeavesTheTemplateUntouched is AC-45's third
// exclusion: an unlisted agent and a listed agent whose definitions are empty
// both render the policy-free Job byte for byte.
func TestJob14MissOrEmptyDefinitionLeavesTheTemplateUntouched(t *testing.T) {
	cases := []struct {
		name   string
		policy string
	}{
		{
			name:   "unlisted agent",
			policy: "agents:\n  other:\n    resourcePolicy: agent\n    resources: { requests: { cpu: \"9\" } }\n",
		},
		{
			name: "empty definition bodies",
			policy: `
agents:
  omp:
    resourcePolicy: agent
    capPolicy: agent
    nodeSelectorPolicy: agent
    resources: {}
    capabilities: { add: [] }
    nodeSelector: {}
`,
		},
	}
	templatePath := deployedTemplatePath(t)
	plain, _ := tcPolicyAxisBuilder(t, templatePath, "", nil)
	baseline := tcJob14Build(t, plain)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			policyPath := writePolicyFile(t, tc.policy)
			withPolicy, logs := tcPolicyAxisBuilder(t, templatePath, policyPath, nil)
			job := tcJob14Build(t, withPolicy)

			if a, b := tcJob14JSON(t, baseline), tcJob14JSON(t, job); a != b {
				t.Errorf("the Job changed although no axis had a usable definition\n--- without ---\n%s\n--- with ---\n%s", a, b)
			}
			// The equality is a miss, not a silently skipped build: the event
			// records the miss with no face written.
			ev := tcJob14Event(t, logs)
			tcPolicyAxisWantList(t, ev, "template_written_faces")
			if source := tcPolicyAxisField(t, ev, "resource_source"); source != "no_definition" {
				t.Errorf("resource_source = %q, want no_definition", source)
			}
		})
	}
}

// TestJob14OnlyTheThreeFacesDiffer is AC-45's totality clause: with a policy
// that hits all three axes and a resolved session node, the diff against the
// policy-free render contains nothing outside the three A1.5 faces and the
// soft-affinity item the nodeSelector hit suppresses.
func TestJob14OnlyTheThreeFacesDiffer(t *testing.T) {
	templatePath := deployedTemplatePath(t)
	withNodes := func(cfg *jobbuilder.Config) { cfg.Nodes = tcJob14NodeIndex{} }

	plain, _ := tcPolicyAxisBuilder(t, templatePath, "", withNodes)
	policyPath := writePolicyFile(t, tcJob14Policy)
	withPolicy, logs := tcPolicyAxisBuilder(t, templatePath, policyPath, withNodes)

	entry := tcJob14Entry()
	entry.PriorSessionID = "sess-1"
	baseline, _, _, err := plain.Build(entry)
	if err != nil {
		t.Fatalf("Build without policy: %v", err)
	}
	job, _, _, err := withPolicy.Build(entry)
	if err != nil {
		t.Fatalf("Build with policy: %v", err)
	}

	// The suppression is a named face, not a stray difference: the policy-free
	// render names the session's node, the policy-hit one has no item left.
	if affinity := baseline.Spec.Template.Spec.Affinity; affinity == nil || affinity.NodeAffinity == nil ||
		len(affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution) != 1 {
		t.Fatalf("the policy-free render lost the resolved <preferred node> item: %+v", affinity)
	}
	if affinity := job.Spec.Template.Spec.Affinity; affinity != nil {
		t.Errorf("affinity = %+v, want the item dropped by the nodeSelector axis (ADR-036)", affinity)
	}

	allowed := []string{
		"/spec/template/spec/containers/0/resources",
		"/spec/template/spec/containers/0/securityContext/capabilities/add",
		"/spec/template/spec/nodeSelector",
		"/spec/template/spec/affinity",
	}
	var diffs []string
	tcJob14DiffPaths(tcJob14View(t, baseline), tcJob14View(t, job), "", &diffs)
	if len(diffs) == 0 {
		t.Fatal("the policy hit wrote nothing: the diff is empty")
	}
	for _, path := range diffs {
		if !tcJob14AllowedPath(path, allowed) {
			t.Errorf("the policy wrote %s, which is outside the three A1.5 faces: all diffs %v", path, diffs)
		}
	}
	for _, face := range allowed[:3] {
		if !tcJob14HasPrefix(diffs, face) {
			t.Errorf("the hit did not write the face %s: diffs %v", face, diffs)
		}
	}
	if !tcJob14HasPrefix(diffs, allowed[3]) {
		t.Errorf("the suppression is not visible in the diff: %v", diffs)
	}

	ev := tcJob14Event(t, logs)
	tcPolicyAxisWantList(t, ev, "template_written_faces", "resources", "capabilities_add", "node_selector")
	tcPolicyAxisWantField(t, ev, "soft_affinity_suppressed", "policy")
}

// tcJob14Build renders the case's task through one builder.
func tcJob14Build(t *testing.T, builder *jobbuilder.Builder) *batchv1.Job {
	t.Helper()
	entry := tcJob14Entry()
	job, _, _, err := builder.Build(entry)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return job
}

// tcJob14JSON renders one object as canonical JSON.
func tcJob14JSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(raw)
}

// tcJob14View renders one Job as a generic JSON view for path-wise diffing.
func tcJob14View(t *testing.T, job *batchv1.Job) any {
	t.Helper()
	raw, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("marshal Job: %v", err)
	}
	var view any
	if err := json.Unmarshal(raw, &view); err != nil {
		t.Fatalf("unmarshal Job: %v", err)
	}
	return view
}

// tcJob14DiffPaths collects the JSON paths at which two views differ.
func tcJob14DiffPaths(want, got any, prefix string, out *[]string) {
	switch wantValue := want.(type) {
	case map[string]any:
		gotValue, ok := got.(map[string]any)
		if !ok {
			*out = append(*out, prefix)
			return
		}
		keys := make(map[string]bool, len(wantValue)+len(gotValue))
		for key := range wantValue {
			keys[key] = true
		}
		for key := range gotValue {
			keys[key] = true
		}
		for key := range keys {
			tcJob14DiffPaths(wantValue[key], gotValue[key], prefix+"/"+key, out)
		}
	case []any:
		gotValue, ok := got.([]any)
		if !ok || len(gotValue) != len(wantValue) {
			*out = append(*out, prefix)
			return
		}
		for i := range wantValue {
			tcJob14DiffPaths(wantValue[i], gotValue[i], fmt.Sprintf("%s/%d", prefix, i), out)
		}
	default:
		if want != got {
			*out = append(*out, prefix)
		}
	}
}

// tcJob14AllowedPath reports whether a diff path sits inside one allowed face.
func tcJob14AllowedPath(path string, allowed []string) bool {
	for _, face := range allowed {
		if path == face || strings.HasPrefix(path, face+"/") {
			return true
		}
	}
	return false
}

// tcJob14HasPrefix reports whether any diff path sits inside one face.
func tcJob14HasPrefix(diffs []string, face string) bool {
	for _, path := range diffs {
		if path == face || strings.HasPrefix(path, face+"/") {
			return true
		}
	}
	return false
}

// tcJob14Capabilities returns one init container's drop (or add) list.
func tcJob14Capabilities(container corev1.Container, drop bool) []string {
	if container.SecurityContext == nil || container.SecurityContext.Capabilities == nil {
		return nil
	}
	list := container.SecurityContext.Capabilities.Add
	if drop {
		list = container.SecurityContext.Capabilities.Drop
	}
	out := make([]string, 0, len(list))
	for _, item := range list {
		out = append(out, string(item))
	}
	return out
}

// tcJob14Event returns the last policy.applied event of one startup log.
func tcJob14Event(t *testing.T, logs *bytes.Buffer) map[string]any {
	t.Helper()
	var found map[string]any
	for _, line := range bytes.Split(logs.Bytes(), []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var fields map[string]any
		if err := json.Unmarshal(line, &fields); err != nil {
			continue
		}
		if fields["msg"] == "policy.applied" {
			found = fields
		}
	}
	if found == nil {
		t.Fatalf("no policy.applied event in the log: %s", logs.String())
	}
	return found
}
