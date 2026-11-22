package release

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestForemanWorkflowBuildsEveryContainerfileInput covers the publish path,
// not a Makefile target: the real workflow's build step must put each per-arch
// file the Foreman Containerfile COPYs into the build context. The image
// carries the server, the Job-side supervisor and foreman-gc; the Job carrier
// is the external rsbx image, so no Containerfile here installs a CLI.
func TestForemanWorkflowBuildsEveryContainerfileInput(t *testing.T) {
	requireTools(t, "bash")
	top := workflowEnv(t, foremanWorkflow)
	steps := workflowSteps(t, foremanWorkflow)
	build := requireStep(t, steps, "Build the per-platform binaries", foremanWorkflow)
	publish := requireStep(t, steps, "Build and push the image", foremanWorkflow)

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
	if len(copies) != 3 {
		t.Fatalf("Containerfile carries %d per-arch COPY inputs, want foreman, foreman-job and foreman-gc", len(copies))
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
	for _, want := range []string{"foreman", "foreman-job", "foreman-gc"} {
		if !seen[want] {
			t.Fatalf("Containerfile COPY inputs = %v, want %s", seen, want)
		}
	}
	if strings.Contains(string(body), "multica") {
		t.Fatal("the Foreman image installs a Multica CLI; the carrier fetches it through the proxy")
	}
	if !strings.Contains(publish.run, "--push dist") {
		t.Fatalf("the publish step does not use dist as its build context:\n%s", publish.run)
	}

	log, err := os.ReadFile(buildLog)
	if err != nil {
		t.Fatalf("the build step never invoked go: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(log)), "\n")
	for _, arch := range []string{"amd64", "arm64"} {
		for _, cmd := range []string{"foreman", "foreman-job", "foreman-gc"} {
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
