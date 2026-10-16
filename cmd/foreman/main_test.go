package main

import (
	"testing"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

// TestRegisterSourceViewDeclaresFixedRuntimeName pins S31's take-off face for
// AC-37: the projection reports the startup discovery verbatim (the name the
// probe reported for its own pod) while its declaration entries carry Foreman's
// fixed runtime display name (§1.1 C1 的取值来源与声明域, ADR-031).
func TestRegisterSourceViewDeclaresFixedRuntimeName(t *testing.T) {
	const probeName = "Oh-My-Pi (company-01)"
	client := proxy.NewClient(proxy.Config{
		ServerURL:   "http://127.0.0.1:1",
		WorkspaceID: "ws-1",
		DaemonID:    "foreman",
	}, "mdt_test")
	client.Declare(registry.Discovery{
		CLIVersion: "18.0.0",
		Runtimes: []registry.DiscoveredRuntime{
			{Name: probeName, Type: "omp", Version: "18.0.0"},
		},
	})

	view, ok := registerSourceView(client, proxy.NewCatalogSlots())().(map[string]any)
	if !ok {
		t.Fatal("S31 view is not an object")
	}
	runtimes, ok := view["runtimes"].([]registry.DiscoveredRuntime)
	if !ok || len(runtimes) != 1 {
		t.Fatalf("S31 runtimes = %#v, want the discovery's single entry", view["runtimes"])
	}
	if runtimes[0].Name != probeName {
		t.Errorf("S31 runtimes[0].name = %q, want the probe's own reported name %q", runtimes[0].Name, probeName)
	}
	declaration, ok := view["declaration"].([]map[string]string)
	if !ok || len(declaration) != 1 {
		t.Fatalf("S31 declaration = %#v, want one entry per declared runtime", view["declaration"])
	}
	if got := declaration[0]["name"]; got != "foreman-job" {
		t.Errorf("S31 declaration[0].name = %q, want the fixed runtime display name", got)
	}
}
