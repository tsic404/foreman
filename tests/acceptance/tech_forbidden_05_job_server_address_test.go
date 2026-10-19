// TC-tech-forbidden-05 (F5): a Job's daemon dials Foreman, never the real
// server. The rendered MULTICA_SERVER_URL equals the deployment's Job-side
// entry point (the Foreman Service), no env value carries the upstream URL,
// and the only env writes Foreman makes are the three injected keys — every
// other entry is the template's verbatim.
package acceptance

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"

	"github.com/tsic404/foreman/internal/jobbuilder"
)

// tcForbidden05Deployment reads the Foreman Deployment's env and Service.
func tcForbidden05Deployment(t *testing.T) (map[string]string, corev1.Service) {
	t.Helper()
	env := map[string]string{}
	var service corev1.Service
	foundService := false
	for _, document := range yamlDocuments(t, filepath.Join("..", "..", "deploy", "30-foreman.yaml")) {
		jsonBytes, err := utilyaml.ToJSON(document)
		if err != nil {
			t.Fatalf("decode deploy/30-foreman.yaml: %v", err)
		}
		var kind struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(jsonBytes, &kind); err != nil {
			t.Fatalf("decode kind: %v", err)
		}
		switch kind.Kind {
		case "Deployment":
			var deployment appsv1.Deployment
			if err := json.Unmarshal(jsonBytes, &deployment); err != nil {
				t.Fatalf("decode Deployment: %v", err)
			}
			for _, entry := range deployment.Spec.Template.Spec.Containers[0].Env {
				env[entry.Name] = entry.Value
			}
		case "Service":
			if err := json.Unmarshal(jsonBytes, &service); err != nil {
				t.Fatalf("decode Service: %v", err)
			}
			foundService = true
		}
	}
	if !foundService {
		t.Fatal("deploy/30-foreman.yaml carries no Service")
	}
	return env, service
}

// tcForbidden05EnvEqual compares the value and the source of two env entries.
func tcForbidden05EnvEqual(a, b corev1.EnvVar) bool {
	if a.Value != b.Value {
		return false
	}
	if (a.ValueFrom == nil) != (b.ValueFrom == nil) {
		return false
	}
	if a.ValueFrom == nil {
		return true
	}
	ar, br := a.ValueFrom.FieldRef, b.ValueFrom.FieldRef
	if (ar == nil) != (br == nil) {
		return false
	}
	return ar == nil || ar.FieldPath == br.FieldPath
}

func TestForbidden05JobDialsForemanOnly(t *testing.T) {
	env, service := tcForbidden05Deployment(t)
	serverURL := env["FOREMAN_JOB_SERVER_URL"]
	if serverURL != jobbuilder.DefaultJobServerURL {
		t.Errorf("deployment FOREMAN_JOB_SERVER_URL = %q, want %q", serverURL, jobbuilder.DefaultJobServerURL)
	}
	wantURL := "http://" + service.Name + "." + service.Namespace + ".svc.cluster.local:8080"
	if serverURL != wantURL {
		t.Errorf("FOREMAN_JOB_SERVER_URL = %q, want the Service address %q", serverURL, wantURL)
	}

	h := newHarness(t, false)
	h.useRealBuilder(t)
	tcPrepareRuntimes(t, h)
	tcClaimOnClaim(t, h, tcBaseClaim("task-1"))
	job := h.createdJob(t, "fm-task-1")
	template := decodeJob(t, []byte(publishedTemplateContent(t)))
	templateAgent := tcJob09Container(t, template.Spec.Template.Spec.Containers, "agent")
	agent := tcJob09Container(t, job.Spec.Template.Spec.Containers, "agent")

	injected := map[string]bool{
		"MULTICA_SERVER_URL":                  true,
		"MULTICA_DAEMON_ID":                   true,
		"MULTICA_DAEMON_MAX_CONCURRENT_TASKS": true,
	}
	templateEnv := map[string]corev1.EnvVar{}
	for _, entry := range templateAgent.Env {
		templateEnv[entry.Name] = entry
	}
	for _, entry := range agent.Env {
		if injected[entry.Name] {
			continue
		}
		want, ok := templateEnv[entry.Name]
		if !ok || !tcForbidden05EnvEqual(entry, want) {
			t.Errorf("env %s was rewritten; Foreman writes only the three injected keys", entry.Name)
		}
	}
	for _, entry := range agent.Env {
		if strings.Contains(entry.Value, h.upstream.server.URL) {
			t.Errorf("env %s points at the real server (%q)", entry.Name, entry.Value)
		}
	}
	server := tcAuth04Env(t, agent.Env, "MULTICA_SERVER_URL")
	if server.Value != jobbuilder.DefaultJobServerURL {
		t.Errorf("rendered MULTICA_SERVER_URL = %q, want the Foreman address %q",
			server.Value, jobbuilder.DefaultJobServerURL)
	}
	if server.Value == h.upstream.server.URL {
		t.Error("the Job dials the real server directly")
	}
}
