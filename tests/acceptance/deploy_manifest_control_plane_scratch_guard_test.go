// Repo-side guard, not a design case: it pins the shipped
// deploy/30-foreman.yaml scratch provision for the control-plane probe. The
// bound probe (cmd/foreman/startup_discovery.go) and the omp adapter
// (internal/discovery/builtin.go) both call os.MkdirTemp("", …), which
// resolves to the process TMPDIR; the container ships a read-only root
// filesystem, so an undeclared scratch turns every verification into
// "read-only file system" and leaves the adapter untrusted forever. Registered
// in acceptanceFixtures, so the census does not count this file as a design
// case's evidence.
package acceptance

import (
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestDeployManifestGivesControlPlaneProbeWritableScratch(t *testing.T) {
	deployment := deployedForemanDeployment(t)
	pod := deployment.Spec.Template.Spec

	var container *corev1.Container
	for i := range pod.Containers {
		if pod.Containers[i].Name == "foreman" {
			container = &pod.Containers[i]
		}
	}
	if container == nil {
		t.Fatal("the Deployment declares no foreman container: the guard reads its scratch provision")
	}

	// The premise of the guard: the root filesystem is not writable, so the
	// image's own /tmp cannot serve as scratch. Removing the hardening without
	// providing another writable location is exactly the regression this holds.
	security := container.SecurityContext
	if security == nil || security.ReadOnlyRootFilesystem == nil || !*security.ReadOnlyRootFilesystem {
		t.Fatal("the foreman container no longer ships readOnlyRootFilesystem: true: re-check this guard's premise before dropping the scratch pin")
	}

	// os.MkdirTemp("", …) reads TMPDIR, and an unset TMPDIR falls back to the
	// read-only /tmp. The manifest must name the directory, not inherit one.
	var scratch string
	for _, env := range container.Env {
		if env.Name != "TMPDIR" {
			continue
		}
		if env.ValueFrom != nil {
			t.Fatal("TMPDIR is not a literal value: the probe resolves it before any Kubernetes field can be read")
		}
		scratch = env.Value
	}
	if scratch == "" {
		t.Fatal(`no TMPDIR: os.MkdirTemp("", …) falls back to /tmp on the read-only root filesystem, so every bound verification fails with "read-only file system" and the adapter is never trusted`)
	}
	if !filepath.IsAbs(scratch) {
		t.Fatalf("TMPDIR = %q, want an absolute path: a relative TMPDIR resolves against an unwritable working directory", scratch)
	}

	// The scratch must be a writable mount with its own bound, or the probe
	// either cannot write at all or fills the durable state volume.
	var mount *corev1.VolumeMount
	for i := range container.VolumeMounts {
		if filepath.Clean(container.VolumeMounts[i].MountPath) == filepath.Clean(scratch) {
			mount = &container.VolumeMounts[i]
		}
	}
	if mount == nil {
		t.Fatalf("TMPDIR = %q but no volume is mounted there: writes still land on the read-only root filesystem", scratch)
	}
	if mount.ReadOnly {
		t.Fatalf("the volume mounted at TMPDIR (%q) is read-only: the probe cannot create its directory", scratch)
	}
	var volume *corev1.Volume
	for i := range pod.Volumes {
		if pod.Volumes[i].Name == mount.Name {
			volume = &pod.Volumes[i]
		}
	}
	if volume == nil {
		t.Fatalf("volume %q is mounted at TMPDIR but not declared", mount.Name)
	}
	if volume.EmptyDir == nil {
		t.Fatalf("volume %q at TMPDIR is not an emptyDir: probe scratch is pod-local and must not alias persistent data", mount.Name)
	}
	if volume.EmptyDir.SizeLimit == nil || volume.EmptyDir.SizeLimit.Sign() <= 0 {
		t.Fatalf("volume %q at TMPDIR has no positive sizeLimit: a repeated verification could fill the node's disk", mount.Name)
	}
}
