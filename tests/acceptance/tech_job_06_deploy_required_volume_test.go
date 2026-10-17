// TC-tech-job-06, container layer: AC-27 names two defenses, and the kubelet-
// side one has its judgment point in the deployment spec — a pod whose template
// ConfigMap is missing must never start. That refusal is the volume's
// `optional: false`, so this case reads deploy/30-foreman.yaml and holds the
// manifest to it: `optional: true`, or a dropped key, turns the pod gate off
// with no other symptom (03-contracts §5.4「启动期判定」/ ADR-030 决策 1；the
// design's cluster check compares the same field's value).
package acceptance

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"

	"github.com/tsic404/foreman/internal/jobbuilder"
)

// templateVolume is the volume name deploy/30-foreman.yaml gives the template
// carrier; the name itself is the deployment's, the ConfigMap's is the
// contract's.
const templateVolume = "job-template"

// templateMountPath is §5.4's fixed mount point, minus the trailing slash the
// design writes for prose.
const templateMountPath = "/etc/foreman/job-template"

func TestJob06DeployTemplateVolumeIsRequired(t *testing.T) {
	deployment := deployedForemanDeployment(t)
	pod := deployment.Spec.Template.Spec

	var volume *corev1.Volume
	for i := range pod.Volumes {
		if candidate := &pod.Volumes[i]; candidate.ConfigMap != nil &&
			candidate.ConfigMap.Name == "foreman-job-template" {
			volume = candidate
		}
	}
	if volume == nil {
		t.Fatal("the Deployment carries no ConfigMap volume for foreman-job-template: with nothing mounted, a missing ConfigMap would start a Foreman that has no template to render from")
	}
	if volume.Name != templateVolume {
		t.Errorf("template volume name = %q, want %q", volume.Name, templateVolume)
	}
	// The key has to be *present* and false. The apiserver default is false
	// too, but the design's cluster check reads this field's value, and a
	// dropped key says nothing about the deployment's intent — the next edit
	// could as well write true.
	if volume.ConfigMap.Optional == nil {
		t.Fatal("the template volume must state optional: false explicitly: a dropped key no longer expresses the required mount (03-contracts §5.4 / ADR-030 决策 1)")
	}
	if *volume.ConfigMap.Optional {
		t.Fatal("the template volume is optional: a missing ConfigMap would let the pod start with no template at all — the container-layer half of the fail-fast pair is gone (ADR-030 决策 1)")
	}

	// The mount must put the ConfigMap's key exactly where the startup gate
	// reads it, or the non-optional volume protects a file nobody opens.
	key := filepath.Base(jobbuilder.TemplatePath)
	mounted := false
	for _, container := range pod.Containers {
		for _, mount := range container.VolumeMounts {
			if mount.Name != volume.Name {
				continue
			}
			mounted = true
			if got := strings.TrimSuffix(mount.MountPath, "/"); got != templateMountPath {
				t.Errorf("container %q template mountPath = %q, want %q", container.Name, mount.MountPath, templateMountPath)
			}
			if !mount.ReadOnly {
				t.Errorf("container %q template mount is writable, want read-only", container.Name)
			}
			if got, want := filepath.Join(mount.MountPath, key), jobbuilder.TemplatePath; got != want {
				t.Errorf("container %q template file resolves to %q, want the startup gate's %q", container.Name, got, want)
			}
		}
	}
	if !mounted {
		t.Fatalf("no container mounts the template volume %q: the startup gate reads %q", volume.Name, jobbuilder.TemplatePath)
	}
}

// deployedForemanDeployment decodes the Deployment out of deploy/30-foreman.yaml.
func deployedForemanDeployment(t *testing.T) appsv1.Deployment {
	t.Helper()
	for _, doc := range yamlDocuments(t, filepath.Join("..", "..", "deploy", "30-foreman.yaml")) {
		jsonBytes, err := utilyaml.ToJSON(doc)
		if err != nil {
			t.Fatalf("deploy/30-foreman.yaml document is not valid YAML: %v", err)
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
	t.Fatal("deploy/30-foreman.yaml carries no Deployment")
	return appsv1.Deployment{}
}
