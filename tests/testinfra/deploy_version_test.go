// The test cluster's server follows upstream's `latest` tag; every other
// image stays pinned. The server is the artifact the acceptance round is
// about, so it must track the newest release — and because `latest` moves,
// the manifest pulls it Always and the round records the digest it got. A
// floating fixture (database, proxy, model backend) would make a failure
// unattributable, so the manifest is judged here instead of against a live
// pull.
package testinfra

import (
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

const scriptedBackendManifest = "../../testinfra/test-multica/scripted-backend.yaml"

// manifestContainers reads every container (init containers included) of every
// Deployment in a manifest, keyed by `deployment/container`.
func manifestContainers(t *testing.T, path string) map[string]map[string]any {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	containers := map[string]map[string]any{}
	decoder := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(string(body)), 4096)
	for {
		var doc unstructured.Unstructured
		err := decoder.Decode(&doc)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		if doc.GetKind() != "Deployment" {
			continue
		}
		for _, field := range []string{"containers", "initContainers"} {
			items, _, _ := unstructured.NestedSlice(doc.Object, "spec", "template", "spec", field)
			for _, item := range items {
				container, _ := item.(map[string]any)
				name, _ := container["name"].(string)
				containers[doc.GetName()+"/"+name] = container
			}
		}
	}
	return containers
}

// TestServerFollowsLatestWhileFixturesStayPinned pins the manifest decision:
// the two upstream server images carry the movable `latest` tag and pull
// Always, so a node cache cannot answer for it; every other container carries
// an exact release tag.
func TestServerFollowsLatestWhileFixturesStayPinned(t *testing.T) {
	server := map[string]string{
		"backend/backend": "ghcr.io/multica-ai/multica-backend:latest",
		"web/web":         "ghcr.io/multica-ai/multica-web:latest",
	}
	seen := map[string]bool{}
	for _, path := range []string{deployManifest, modelBackendManifest, scriptedBackendManifest} {
		containers := manifestContainers(t, path)
		if len(containers) == 0 {
			t.Fatalf("%s declares no Deployment containers", path)
		}
		for name, container := range containers {
			image, _ := container["image"].(string)
			want, isServer := server[name]
			if isServer {
				if path != deployManifest {
					t.Errorf("%s is declared outside %s", name, deployManifest)
				}
				seen[name] = true
				if image != want {
					t.Errorf("%s image is %q, want the floating %q", name, image, want)
				}
				if policy, _ := container["imagePullPolicy"].(string); policy != "Always" {
					t.Errorf("%s imagePullPolicy is %q, want Always — a movable tag with a cached image would run another build", name, policy)
				}
				continue
			}
			if image == "" {
				t.Errorf("%s in %s declares no image", name, path)
				continue
			}
			if strings.HasSuffix(image, ":latest") || !strings.Contains(image, ":") {
				t.Errorf("%s in %s carries %q — only the test server follows latest; a fixture must stay pinned to an exact release", name, path, image)
			}
		}
	}
	for name := range server {
		if !seen[name] {
			t.Errorf("%s is not declared in %s", name, deployManifest)
		}
	}
}
