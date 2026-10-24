// TC-tech-cache-06 / AC-26 (multi-instance variant): the shared index's
// retention is the store's own TTL, so a row written by one instance is
// readable by the other and disappears once that lifetime ends (§4.5 索引).
// The writer-clock-skew invariant — a peer's stamp never expires a live row —
// is pinned by internal/registry's unit test, not by this case.
package acceptance

import (
	"encoding/json"
	"testing"
	"time"
)

func TestTechCache06SessionIndexSharedAndExpired(t *testing.T) {
	tw := newTwoInstances(t, twoInstanceOptions{})
	ctx := t.Context()
	raw, err := json.Marshal(map[string]any{
		"workspace_id": "ws-1",
		"key":          "sess-old",
		"node":         "node-a",
		"learned_at":   time.Now(),
	})
	if err != nil {
		t.Fatalf("marshal session row: %v", err)
	}
	if err := tw.a.coord.PutSessionNode(ctx, "ws-1", "sess-old", raw, time.Hour); err != nil {
		t.Fatalf("PutSessionNode: %v", err)
	}
	if got := tw.b.reg.LastNodeForSession("ws-1", "sess-old"); got != "node-a" {
		t.Fatalf("cross-instance session lookup = %q, want node-a", got)
	}
	tw.mr.FastForward(time.Hour + time.Second)
	if _, ok, err := tw.a.coord.GetSessionNode(ctx, "ws-1", "sess-old"); err != nil || ok {
		t.Fatalf("shared session row survived its TTL (ok=%v err=%v)", ok, err)
	}
	if got := tw.a.reg.LastNodeForSession("ws-1", "sess-old"); got != "" {
		t.Fatalf("lookup after the TTL = %q, want a miss", got)
	}
}
