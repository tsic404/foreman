// TC-tech-forbidden-08 (F8): Foreman touches only the Jobs and Secrets it
// created. The RBAC face has no cluster-scoped role and no wildcard, and the
// Job pods carry no API credential. The behavior face is the scheduler's
// object interface: create/delete/get/list on Jobs and Secrets plus pod reads
// — there is no update, patch or arbitrary-object method to call. A completed
// task's deletions name only its own objects.
package acceptance

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	utilyaml "k8s.io/apimachinery/pkg/util/yaml"

	"github.com/tsic404/foreman/internal/scheduler"
)

// tcForbidden08RBACObject is the generic shape of an RBAC document.
type tcForbidden08RBACObject struct {
	Kind     string `json:"kind"`
	Metadata struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"metadata"`
	Automount *bool `json:"automountServiceAccountToken"`
	Rules     []struct {
		APIGroups []string `json:"apiGroups"`
		Resources []string `json:"resources"`
		Verbs     []string `json:"verbs"`
	} `json:"rules"`
	RoleRef struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
	} `json:"roleRef"`
	Subjects []struct {
		Kind      string `json:"kind"`
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"subjects"`
}

func TestForbidden08ClusterObjectScope(t *testing.T) {
	allowedResources := map[string]bool{"secrets": true, "jobs": true, "pods": true, "pods/log": true}
	allowedVerbs := map[string]bool{"create": true, "delete": true, "get": true, "list": true, "watch": true}
	serviceAccounts := map[string]*bool{}

	for _, document := range yamlDocuments(t, filepath.Join("..", "..", "deploy", "20-rbac.yaml")) {
		jsonBytes, err := utilyaml.ToJSON(document)
		if err != nil {
			t.Fatalf("decode deploy/20-rbac.yaml: %v", err)
		}
		var object tcForbidden08RBACObject
		if err := json.Unmarshal(jsonBytes, &object); err != nil {
			t.Fatalf("decode RBAC object: %v", err)
		}
		switch object.Kind {
		case "ClusterRole", "ClusterRoleBinding":
			t.Errorf("cluster-scoped RBAC object %s/%s", object.Kind, object.Metadata.Name)
		case "Role":
			if object.Metadata.Namespace != "multica-agents" {
				t.Errorf("Role %s namespace = %q, want multica-agents", object.Metadata.Name, object.Metadata.Namespace)
			}
			for _, rule := range object.Rules {
				for _, resource := range rule.Resources {
					if resource == "*" || !allowedResources[resource] {
						t.Errorf("Role rule resource %q is outside the Job/Secret/pod face", resource)
					}
				}
				for _, verb := range rule.Verbs {
					if verb == "*" || !allowedVerbs[verb] {
						t.Errorf("Role rule verb %q is outside create/delete/get/list/watch", verb)
					}
				}
				for _, group := range rule.APIGroups {
					if group == "*" || (group != "" && group != "batch") {
						t.Errorf("Role rule apiGroup %q is outside the core/batch face", group)
					}
				}
			}
		case "ServiceAccount":
			serviceAccounts[object.Metadata.Name] = object.Automount
		case "RoleBinding":
			if object.RoleRef.Kind != "Role" {
				t.Errorf("RoleBinding %s references %s, want a namespaced Role", object.Metadata.Name, object.RoleRef.Kind)
			}
			for _, subject := range object.Subjects {
				if subject.Kind != "ServiceAccount" || subject.Name != "foreman" {
					t.Errorf("RoleBinding grants %s/%s; only the foreman ServiceAccount may hold it",
						subject.Kind, subject.Name)
				}
			}
		}
	}
	if automount := serviceAccounts["multica-omp"]; automount == nil || *automount {
		t.Error("the Job ServiceAccount mounts an API token; Job pods must carry no API credential")
	}

	// The behavior face: the scheduler's object interface has no method that
	// could modify an object it did not create.
	wantMethods := map[string]bool{
		"CreateJob": true, "DeleteJob": true, "GetJob": true,
		"CreateSecret": true, "DeleteSecret": true,
		"ListJobs": true, "ListSecrets": true, "ListPods": true,
	}
	interfaceType := reflect.TypeOf((*scheduler.JobClient)(nil)).Elem()
	for i := range interfaceType.NumMethod() {
		name := interfaceType.Method(i).Name
		if !wantMethods[name] {
			t.Errorf("JobClient exposes %s, which is outside the self-created Job/Secret face", name)
		}
		delete(wantMethods, name)
	}
	for name := range wantMethods {
		t.Errorf("JobClient lost %s", name)
	}

	// A completed task's deletions name only its own objects.
	h := newDeliveryHarness(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)
	claimTask(t, h, "task-1", token)
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/complete", token, []byte(`{"exit_code":0}`)); code != 200 {
		t.Fatalf("S13 = %d (%s), want 200", code, body)
	}
	waitTerminal(t, h, "task-1")
	for _, name := range h.jobs.deletedJobNames() {
		if !strings.HasPrefix(name, "fm-task-1") {
			t.Errorf("the scheduler deleted a Job it did not create: %s", name)
		}
	}
	// The credential clear may precede its create, so the task's identity
	// namespace counts as its own; any other Secret must be an object this
	// scheduler created here (the pending-report carrier, ADR-046).
	createdSecrets := make(map[string]bool, len(h.jobs.createdSecrets))
	for _, secret := range h.jobs.createdSecrets {
		createdSecrets[secret.Name] = true
	}
	for _, name := range h.jobs.deletedSecretNames() {
		if strings.HasPrefix(name, "fm-task-1") || createdSecrets[name] {
			continue
		}
		t.Errorf("the scheduler deleted a Secret it did not create: %s", name)
	}
}
