// Repo-side guard, not a design case: it pins the control-plane image's
// runtime dependency. The bound probe executes the verified omp payload in the
// control plane (cmd/foreman/startup_discovery.go), and that binary's ELF
// needs libstdc++.so.6. Alpine's base image does not carry it, so an image
// without the package writes the artifact and then fails at the dynamic
// loader: no version, no declaration, adapter never trusted. Registered in
// acceptanceFixtures, so the census does not count this file as a design
// case's evidence.
package acceptance

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestForemanImageCarriesTheOMPRuntime(t *testing.T) {
	path := filepath.Join("..", "..", "images", "foreman.Containerfile")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	// Only non-comment apk invocations count: a commented-out install is not a
	// delivered library, and neither is a line that merely names it.
	installed := false
	for _, line := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") || !strings.Contains(trimmed, "apk add") {
			continue
		}
		for _, field := range strings.Fields(trimmed) {
			if strings.Trim(field, `;\`) == "libstdc++" {
				installed = true
			}
		}
	}
	if !installed {
		t.Fatal("images/foreman.Containerfile does not install libstdc++: the bound probe executes the verified omp payload, whose ELF needs libstdc++.so.6, so the adapter never reaches trusted")
	}
}
