// TC-tech-job-08 covers the agent-backend provision ADR-033 splits in two: the
// Job image carries `dumb-init` and no longer carries `omp` / `nix`, and the
// reference template installs the agent backend where the agent container's
// MULTICA_OMP_PATH reads it. The image half runs against the published
// manifest list in the release workflow (this suite has no registry); the
// template half runs here, with the install step executed against a stub
// upstream — the asset choice, the checksum gate and the installed mode are
// executable facts, not template prose.
package acceptance

import (
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

// TestJob08ReferenceTemplateProvidesOmpAtOmpPath pins the linkage the daemon
// depends on: the path it is told to read (MULTICA_OMP_PATH on the agent
// container) is a read-only file mount of the file an init container writes
// into the same volume. Either side drifting — the env value or the mount —
// leaves the daemon with no runtime to discover, which the baseline diff alone
// cannot see.
func TestJob08ReferenceTemplateProvidesOmpAtOmpPath(t *testing.T) {
	templateJob := decodeJob(t, []byte(publishedTemplateContent(t)))
	script, installDir, _ := ompInstallStep(t, templateJob)

	pod := templateJob.Spec.Template.Spec
	if len(pod.Containers) == 0 {
		t.Fatal("the reference template declares no containers")
	}
	agent := pod.Containers[0]

	ompPath := ""
	for _, env := range agent.Env {
		if env.Name == "MULTICA_OMP_PATH" {
			ompPath = env.Value
		}
	}
	if ompPath == "" {
		t.Fatal("the agent container declares no MULTICA_OMP_PATH: the daemon would have no agent backend to read")
	}

	var agentMount *corev1.VolumeMount
	for i, mount := range agent.VolumeMounts {
		if mount.MountPath == ompPath {
			agentMount = &agent.VolumeMounts[i]
		}
	}
	if agentMount == nil {
		t.Fatalf("the agent container mounts nothing at MULTICA_OMP_PATH (%s): the env points at a path it cannot see", ompPath)
	}
	if agentMount.SubPath == "" {
		t.Errorf("the mount at %s carries no subPath: it would shadow a whole image directory instead of adding one file", ompPath)
	}
	if !agentMount.ReadOnly {
		t.Errorf("the mount at %s is writable; the agent backend is the install step's to install", ompPath)
	}

	// The init side: the volume the agent reads must be the volume the install
	// step writes into.
	for _, container := range pod.InitContainers {
		for _, mount := range container.VolumeMounts {
			if mount.MountPath != installDir {
				continue
			}
			if mount.Name != agentMount.Name {
				t.Fatalf("the install step writes volume %s while the agent reads volume %s", mount.Name, agentMount.Name)
			}
			if mount.ReadOnly {
				t.Fatalf("the install step mounts volume %s read-only: it cannot install anything", mount.Name)
			}
			installed := path.Base(agentMount.SubPath)
			executable := regexp.MustCompile(`(?m)^chmod 0?755 ` + regexp.QuoteMeta(installDir+"/"+installed) + `$`)
			if !executable.MatchString(script) {
				t.Errorf("the install step does not leave %s/%s executable:\n%s", installDir, installed, script)
			}
			return
		}
	}
	t.Fatalf("no init container writes volume %s at %s (the agent mounts %s from it)", agentMount.Name, installDir, ompPath)
}

// TestJob08OmpInstallStep runs the template's install step against a stub
// upstream serving the release's checksum manifest and both architecture
// assets: the step must fetch the asset matching the machine it runs on,
// verify it against the manifest, leave exactly that file in the mount
// directory and leave it executable. A digest that does not match must end the
// step non-zero without installing an executable.
func TestJob08OmpInstallStep(t *testing.T) {
	templateJob := decodeJob(t, []byte(publishedTemplateContent(t)))
	script, installDir, base := ompInstallStep(t, templateJob)

	// The step runs inside the Job image, whose busybox supplies these.
	for _, tool := range []string{"sh", "wget", "uname", "grep", "cut", "sha256sum", "chmod", "rm"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("host has no %s: the install step is written for the Job image's shell", tool)
		}
	}

	payload := []byte("#!/bin/sh\necho omp\n")
	digest := sha256.Sum256(payload)

	for _, tc := range []struct{ machine, asset string }{
		{machine: "x86_64", asset: "omp-linux-musl-x64"},
		{machine: "aarch64", asset: "omp-linux-musl-arm64"},
	} {
		t.Run(tc.machine, func(t *testing.T) {
			requests, upstream := serveOmpUpstream(t, map[string]string{
				"SHA256SUMS.txt": fmt.Sprintf("%x  %s\n", digest, tc.asset),
				tc.asset:         string(payload),
			})
			dir, out, err := runOmpInstallStep(t, script, installDir, base, upstream, tc.machine)
			if err != nil {
				t.Fatalf("install step failed for %s: %v\n%s", tc.machine, err, out)
			}
			if got := strings.Join(*requests, " "); got != "SHA256SUMS.txt "+tc.asset {
				t.Errorf("install step fetched %q, want the manifest then %s", got, tc.asset)
			}
			installed := filepath.Join(dir, "omp")
			got, err := os.ReadFile(installed)
			if err != nil {
				t.Fatalf("read the installed agent backend: %v", err)
			}
			if string(got) != string(payload) {
				t.Errorf("installed agent backend = %q, want the verified asset bytes", got)
			}
			info, err := os.Stat(installed)
			if err != nil {
				t.Fatalf("stat the installed agent backend: %v", err)
			}
			if info.Mode().Perm()&0o111 == 0 {
				t.Errorf("installed agent backend mode = %v, want it executable", info.Mode().Perm())
			}
			if _, err := os.Stat(filepath.Join(dir, "SHA256SUMS.txt")); !os.IsNotExist(err) {
				t.Errorf("the checksum manifest is still in the mount directory (%v): only the backend belongs there", err)
			}
			if out, err := exec.Command(installed).CombinedOutput(); err != nil || strings.TrimSpace(string(out)) != "omp" {
				t.Errorf("the installed agent backend does not run: %v (%q)", err, out)
			}
		})
	}

	t.Run("checksum mismatch", func(t *testing.T) {
		_, upstream := serveOmpUpstream(t, map[string]string{
			"SHA256SUMS.txt":     fmt.Sprintf("%064d  omp-linux-musl-x64\n", 0),
			"omp-linux-musl-x64": string(payload),
		})
		dir, out, err := runOmpInstallStep(t, script, installDir, base, upstream, "x86_64")
		if err == nil {
			t.Fatalf("install step accepted an asset whose digest is not in the manifest\n%s", out)
		}
		if info, statErr := os.Stat(filepath.Join(dir, "omp")); statErr == nil && info.Mode().Perm()&0o111 != 0 {
			t.Errorf("the unverified asset was left executable (mode %v)", info.Mode().Perm())
		}
	})
}

// ompInstallStep locates the template's agent-backend install step by
// behaviour (it reads the release's checksum manifest and names both
// architecture assets) and returns its script, the directory it installs into
// and the upstream base it reads — so a renamed container does not break the
// case's intent, while a step that stops checking checksums does.
func ompInstallStep(t *testing.T, job batchv1.Job) (script, installDir, base string) {
	t.Helper()
	for _, container := range job.Spec.Template.Spec.InitContainers {
		for _, arg := range container.Args {
			if !strings.Contains(arg, "SHA256SUMS.txt") ||
				!strings.Contains(arg, "omp-linux-musl-x64") ||
				!strings.Contains(arg, "omp-linux-musl-arm64") {
				continue
			}
			if len(container.VolumeMounts) != 1 {
				t.Fatalf("init container %q mounts %d volumes, want the one install directory",
					container.Name, len(container.VolumeMounts))
			}
			match := regexp.MustCompile(`(?m)^base=(\S+)\s*$`).FindStringSubmatch(arg)
			if match == nil {
				t.Fatalf("init container %q names no upstream base:\n%s", container.Name, arg)
			}
			installDir = container.VolumeMounts[0].MountPath
			if !strings.Contains(arg, installDir+"/") {
				t.Fatalf("init container %q mounts %s but its step installs elsewhere:\n%s", container.Name, installDir, arg)
			}
			return arg, installDir, match[1]
		}
	}
	t.Fatal("the reference template carries no step installing the agent backend")
	return "", "", ""
}

// serveOmpUpstream starts a stub standing in for the upstream release. The
// install step is pointed at it, so the case downloads nothing.
func serveOmpUpstream(t *testing.T, served map[string]string) (*[]string, string) {
	t.Helper()
	var requests []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := path.Base(r.URL.Path)
		requests = append(requests, name)
		body, ok := served[name]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(upstream.Close)
	return &requests, upstream.URL
}

// runOmpInstallStep executes the install step the way the Job image does
// (`sh -ec`), against the stub upstream and a temp directory standing in for
// the mounted volume. PATH is prefixed with a `uname` shim so the machine the
// step selects its asset for is the case's input, not the test host's.
func runOmpInstallStep(t *testing.T, script, installDir, base, upstream, machine string) (dir string, out []byte, err error) {
	t.Helper()
	dir = t.TempDir()
	shim := t.TempDir()
	uname := filepath.Join(shim, "uname")
	if writeErr := os.WriteFile(uname, []byte("#!/bin/sh\necho "+machine+"\n"), 0o755); writeErr != nil {
		t.Fatalf("write the uname shim: %v", writeErr)
	}
	rewritten := strings.ReplaceAll(script, base, upstream)
	rewritten = strings.ReplaceAll(rewritten, installDir, dir)
	cmd := exec.Command("sh", "-ec", rewritten)
	cmd.Env = append(os.Environ(), "PATH="+shim+string(os.PathListSeparator)+os.Getenv("PATH"))
	out, err = cmd.CombinedOutput()
	return dir, out, err
}
