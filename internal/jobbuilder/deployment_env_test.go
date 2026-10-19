package jobbuilder

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

// deploymentManifest is the Deployment this module's configuration must match
// in one direction only: no key Foreman stopped reading may stay behind. An
// omitted key is legal — its default applies — so this is not a declaration
// that every key read here is spelled out in the manifest.
const deploymentManifest = "../../deploy/30-foreman.yaml"

// removedJobShapeEnv lists the env ADR-030 deleted: decision 4's four
// template-tuning keys and decision 5's five resource / nodeSelector keys.
// The template is the only Job shape source, so carrying one of them again is
// silent dead configuration — LoadConfig never reads it (the other half of
// this pin, TestLoadConfigIgnoresTemplateShapeKeys), and startup accepts any
// value, so no other check in CI would notice the regression.
var removedJobShapeEnv = []string{
	"FOREMAN_JOB_IMAGE_PULL_SECRETS",
	"FOREMAN_JOB_TOLERATIONS",
	"FOREMAN_JOB_TTL_SECONDS",
	"FOREMAN_REPO_CACHE_MODE",
	"FOREMAN_JOB_CPU_REQUEST",
	"FOREMAN_JOB_MEMORY_REQUEST",
	"FOREMAN_JOB_CPU_LIMIT",
	"FOREMAN_JOB_MEMORY_LIMIT",
	"FOREMAN_JOB_NODE_SELECTOR",
}

// TestDeploymentOmitsRemovedJobShapeEnv holds the manifest side of the pin: a
// key re-added to the Foreman Deployment's env would be a setting an operator
// can change with no effect, which is exactly the silent dead configuration
// ADR-030 decision 5 removes.
func TestDeploymentOmitsRemovedJobShapeEnv(t *testing.T) {
	deployment := deployedForeman(t)
	declared := make(map[string]struct{})
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if container.Name != "foreman" {
			continue
		}
		for _, env := range container.Env {
			declared[env.Name] = struct{}{}
		}
	}
	if len(declared) == 0 {
		t.Fatal("the foreman container declares no env: the absence check would pass vacuously")
	}
	for _, removed := range removedJobShapeEnv {
		if _, ok := declared[removed]; ok {
			t.Errorf("deploy/30-foreman.yaml declares %s: the key has no consumer (the Job shape comes from the template), so it is silent dead configuration", removed)
		}
	}
}

// deployedForeman decodes the Deployment out of the deployment manifest.
func deployedForeman(t *testing.T) appsv1.Deployment {
	t.Helper()
	raw, err := os.ReadFile(deploymentManifest)
	if err != nil {
		t.Fatalf("read %s: %v", deploymentManifest, err)
	}
	reader := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(raw)))
	for {
		doc, err := reader.Read()
		if err != nil {
			break
		}
		if len(bytes.TrimSpace(doc)) == 0 {
			continue
		}
		jsonBytes, err := utilyaml.ToJSON(doc)
		if err != nil {
			t.Fatalf("%s document is not valid YAML: %v", deploymentManifest, err)
		}
		var typeMeta metav1.TypeMeta
		if err := json.Unmarshal(jsonBytes, &typeMeta); err != nil {
			t.Fatalf("decode document type: %v", err)
		}
		if typeMeta.Kind != "Deployment" {
			continue
		}
		var deployment appsv1.Deployment
		if err := json.Unmarshal(jsonBytes, &deployment); err != nil {
			t.Fatalf("decode the Deployment: %v", err)
		}
		return deployment
	}
	t.Fatalf("%s carries no Deployment", deploymentManifest)
	return appsv1.Deployment{}
}
