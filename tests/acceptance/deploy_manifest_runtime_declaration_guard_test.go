// Repo-side guard, not a design case: it pins the manifest's half of the
// runtime declaration chain. The image installs no Agent CLI, so the manifest
// must not point the probe at one, must mount the credential directory it
// configures, and must size memory for one buffered artifact. A missing mount
// or an undersized limit fails as an unresolvable credential or an OOM kill,
// not as a declaration error. Registered in acceptanceFixtures, so the census
// does not count it as a design case's evidence.
package acceptance

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/release"
)

const catalogCredentialsDir = "/etc/foreman/catalog-credentials"

func TestDeployManifestWiresTheRuntimeDeclarationChain(t *testing.T) {
	deployment := deployedForemanDeployment(t)
	pod := deployment.Spec.Template.Spec

	var container *corev1.Container
	for i := range pod.Containers {
		if pod.Containers[i].Name == "foreman" {
			container = &pod.Containers[i]
		}
	}
	if container == nil {
		t.Fatal("the Deployment declares no foreman container")
	}

	// The image carries no Agent CLI. A literal probe path would fail every
	// startup and leave omp untrusted until a release was published anyway.
	for _, env := range container.Env {
		if env.Name == "FOREMAN_OMP_BINARY" {
			t.Fatalf("FOREMAN_OMP_BINARY = %q: the reference image installs no Agent CLI, so trust must come from the verified published artifact", env.Value)
		}
	}

	// The configured credentials directory must be mounted, or every source
	// registration answers with an unresolvable credential_ref.
	envSet := false
	for _, env := range container.Env {
		if env.Name != "FOREMAN_SOURCE_CREDENTIALS_DIR" {
			continue
		}
		envSet = true
		if env.Value != catalogCredentialsDir {
			t.Errorf("FOREMAN_SOURCE_CREDENTIALS_DIR = %q, want %q", env.Value, catalogCredentialsDir)
		}
	}
	if !envSet {
		t.Fatalf("no FOREMAN_SOURCE_CREDENTIALS_DIR: the management surface reads its per-source credential files from there")
	}

	var mount *corev1.VolumeMount
	for i := range container.VolumeMounts {
		if candidate := &container.VolumeMounts[i]; candidate.MountPath == catalogCredentialsDir {
			mount = candidate
		}
	}
	if mount == nil {
		t.Fatalf("no volume is mounted at %s: registrations would never resolve their credential_ref", catalogCredentialsDir)
	}
	if !mount.ReadOnly {
		t.Errorf("the credential mount %q is writable, want read-only", mount.Name)
	}

	var volume *corev1.Volume
	for i := range pod.Volumes {
		if candidate := &pod.Volumes[i]; candidate.Name == mount.Name {
			volume = candidate
		}
	}
	if volume == nil {
		t.Fatalf("volume %q is mounted but not declared", mount.Name)
	}
	if volume.Secret == nil || volume.Secret.SecretName != "foreman-catalog-credentials" {
		t.Fatalf("credential volume %q is not the foreman-catalog-credentials Secret", volume.Name)
	}
	// Optional on purpose: an installation with no catalog source must still
	// serve, and an absent Secret must not turn into CreateContainerConfigError.
	if volume.Secret.Optional == nil || !*volume.Secret.Optional {
		t.Error("the credential Secret is not optional: a deployment with no catalog source could not start")
	}

	// Verification buffers one artifact in the control plane. A memory limit
	// below the bound turns a publish into an OOM kill instead of a release.
	limit, ok := container.Resources.Limits[corev1.ResourceMemory]
	if !ok {
		t.Fatal("the foreman container declares no memory limit: one verification download is buffered in memory")
	}
	if limit.Value() < release.MaxArtifactBytes {
		t.Fatalf("memory limit = %d bytes, want at least the %d byte verification bound", limit.Value(), release.MaxArtifactBytes)
	}
}
