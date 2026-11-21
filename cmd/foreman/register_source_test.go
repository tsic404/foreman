package main

import (
	"testing"

	"github.com/tsic404/foreman/internal/proxy"
)

// TestRegisterSourceViewKeepsSameWorkspaceSourcesApart pins S31's key: two
// servers exposing the same workspace id are two views, not one row that
// silently overwrites the other.
func TestRegisterSourceViewKeepsSameWorkspaceSourcesApart(t *testing.T) {
	clientA := proxy.NewClient(proxy.Config{SourceID: "src-a", WorkspaceID: "ws-shared", DaemonID: "foreman-a"}, "token-a")
	clientB := proxy.NewClient(proxy.Config{SourceID: "src-b", WorkspaceID: "ws-shared", DaemonID: "foreman-b"}, "token-b")
	clients, err := proxy.NewClientSet(clientA, clientB)
	if err != nil {
		t.Fatalf("client set: %v", err)
	}
	slots := map[string]*proxy.CatalogSlots{
		clientA.ScopeKey(): proxy.NewCatalogSlots(),
		clientB.ScopeKey(): proxy.NewCatalogSlots(),
	}

	view := registerSourceView(clients, slots)()
	if _, ok := view["src-a/ws-shared"]; !ok {
		t.Errorf("source A view missing: %v", view)
	}
	if _, ok := view["src-b/ws-shared"]; !ok {
		t.Errorf("source B view missing: %v", view)
	}
	if len(view) != 2 {
		t.Errorf("view entries = %d, want 2", len(view))
	}
}
