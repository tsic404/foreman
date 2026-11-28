package release

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestForemanWorkflowsBuildEveryContainerfileInput covers both image checks,
// not a Makefile target: every workflow that builds the Foreman image must put
// each per-arch file the Containerfile COPYs into the build context. The image
// carries the server, the Job-side supervisor, foreman-gc and the reference
// Job adapters (V2-C29); the Job carrier is the external rsbx image, so no
// Containerfile here installs a CLI.
func TestForemanWorkflowsBuildEveryContainerfileInput(t *testing.T) {
	requireTools(t, "bash")
	for _, workflow := range []struct {
		name    string
		path    string
		publish string
		pushed  bool
	}{
		{name: "release", path: foremanWorkflow, publish: "Build and push the image", pushed: true},
		{name: "pr-check", path: prCheckWorkflow, publish: "Build the image (no push)"},
	} {
		t.Run(workflow.name, func(t *testing.T) {
			checkWorkflowBuildsEveryContainerfileInput(t, workflow.path, workflow.publish, workflow.pushed)
		})
	}
}

func checkWorkflowBuildsEveryContainerfileInput(t *testing.T, workflowPath, publishStep string, pushed bool) {
	t.Helper()
	top := workflowEnv(t, workflowPath)
	steps := workflowSteps(t, workflowPath)
	build := requireStep(t, steps, "Build the per-platform binaries", workflowPath)
	publish := requireStep(t, steps, publishStep, workflowPath)

	repo := t.TempDir()
	stubDir := t.TempDir()
	buildLog := filepath.Join(stubDir, "go.log")
	stub(t, stubDir, "go", `#!/bin/sh
set -eu
out=
prev=
for arg in "$@"; do
  if [ "$prev" = "-o" ]; then out=$arg; fi
  prev=$arg
done
[ -n "$out" ] || { echo "go stub: no -o argument" >&2; exit 1; }
mkdir -p "$(dirname "$out")"
printf '#!/bin/sh\n' >"$out"
echo "GOARCH=$GOARCH $*" >>"$STUB_GO_LOG"
`)
	// The build step reads the version the resolve step produced; that step is
	// not the subject here, so the case fabricates its output.
	buildEnv := resolveEnv(t, build.env, nil, nil, map[string]string{"version": "v0.0.0-test"})
	runStep(t, build.run, repo, stubDir, mergeEnv(top, buildEnv, map[string]string{"STUB_GO_LOG": buildLog}))

	body, err := os.ReadFile("../../images/foreman.Containerfile")
	if err != nil {
		t.Fatalf("read the Foreman Containerfile: %v", err)
	}
	copies := regexp.MustCompile(`(?m)^COPY\s+([A-Za-z0-9._-]+)-linux-\$\{TARGETARCH\}\s+`).
		FindAllStringSubmatch(string(body), -1)
	want := []string{
		"foreman", "foreman-job", "foreman-gc",
		"foreman-job-cli", "rsbx-remote", "foreman-job-register",
		"foreman-job-runner", "foreman-job-slots",
	}
	if len(copies) != len(want) {
		t.Fatalf("Containerfile carries %d per-arch COPY inputs, want %v", len(copies), want)
	}
	seen := map[string]bool{}
	for _, match := range copies {
		name := match[1]
		seen[name] = true
		for _, arch := range []string{"amd64", "arm64"} {
			path := filepath.Join(repo, "dist", name+"-linux-"+arch)
			if _, err := os.Stat(path); err != nil {
				t.Errorf("build context lacks %s: %v", path, err)
			}
		}
	}
	for _, name := range want {
		if !seen[name] {
			t.Fatalf("Containerfile COPY inputs = %v, want %s", seen, name)
		}
	}
	if strings.Contains(string(body), "multica") {
		t.Fatal("the Foreman image installs a Multica CLI; the carrier fetches it through the proxy")
	}
	if !strings.Contains(publish.run, "dist") {
		t.Fatalf("the image step does not use dist as its build context:\n%s", publish.run)
	}
	if pushed && !strings.Contains(publish.run, "--push dist") {
		t.Fatalf("the publish step does not push dist:\n%s", publish.run)
	}

	log, err := os.ReadFile(buildLog)
	if err != nil {
		t.Fatalf("the build step never invoked go: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(log)), "\n")
	for _, arch := range []string{"amd64", "arm64"} {
		for _, cmd := range want {
			found := false
			for _, line := range lines {
				if strings.Contains(line, "GOARCH="+arch) && strings.Contains(line, "./cmd/"+cmd) {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("build step never built ./cmd/%s for %s:\n%s", cmd, arch, log)
			}
		}
	}
}
