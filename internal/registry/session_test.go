package registry

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestSessionKey(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty is first run", "", ""},
		{"bare id stays", "d41d8cd98f00b204", "d41d8cd98f00b204"},
		{"path keeps base without extension", "/state/home/.multica/pi-sessions/sess-1.jsonl", "sess-1"},
		{"relative path", "home/.multica/pi-sessions/sess-2.jsonl", "sess-2"},
		{"path without extension", "/state/home/.multica/pi-sessions/sess-3", "sess-3"},
		{"only the last dot is the extension", "/a/b/sess.4.jsonl", "sess.4"},
		{"trailing separator", "/a/b/", "b"},
	}
	for _, tc := range cases {
		if got := SessionKey(tc.in); got != tc.want {
			t.Errorf("%s: SessionKey(%q) = %q, want %q", tc.name, tc.in, got, tc.want)
		}
	}
}

func TestLearnSessionAndLookup(t *testing.T) {
	r := New(fixedClock())
	if got := r.LastNodeForSession("ws-1", "sess-1"); got != "" {
		t.Fatalf("unknown key = %q, want empty", got)
	}
	r.LearnSession("ws-1", "", "node-a") // no key
	r.LearnSession("ws-1", "sess-1", "") // no node
	if got := r.LastNodeForSession("ws-1", ""); got != "" {
		t.Fatalf("empty key learned: %q", got)
	}

	r.LearnSession("ws-1", "sess-1", "node-a")
	if got := r.LastNodeForSession("ws-1", "sess-1"); got != "node-a" {
		t.Fatalf("LastNodeForSession = %q, want node-a", got)
	}
	// Re-learning the same key refreshes node and timestamp: the later report
	// says where the session is now.
	r.LearnSession("ws-1", "sess-1", "node-b")
	if got := r.LastNodeForSession("ws-1", "sess-1"); got != "node-b" {
		t.Fatalf("re-learned key = %q, want node-b", got)
	}
}

func TestPruneExpiredSessions(t *testing.T) {
	now := testNow
	r := New(func() time.Time { return now })
	r.LearnSession("ws-1", "stale", "node-a")
	now = now.Add(2 * time.Hour)
	learnedFresh := now
	r.LearnSession("ws-1", "fresh", "node-b")

	if dropped := r.PruneExpiredSessions(time.Hour); dropped != 1 {
		t.Fatalf("dropped = %d, want 1", dropped)
	}
	if got := r.LastNodeForSession("ws-1", "stale"); got != "" {
		t.Fatalf("expired key survived: %q", got)
	}
	if got := r.LastNodeForSession("ws-1", "fresh"); got != "node-b" {
		t.Fatalf("fresh key dropped: %q", got)
	}
	// An entry exactly at the window's edge is still inside it: the rule is
	// `now - learned_at > ttl`.
	now = learnedFresh.Add(time.Hour)
	if dropped := r.PruneExpiredSessions(time.Hour); dropped != 0 {
		t.Fatalf("edge entry dropped: %d", dropped)
	}
	// 0 disables expiry — the index is a hint, so an aged key is only a
	// preference the scheduler may ignore.
	now = now.Add(1000 * time.Hour)
	if dropped := r.PruneExpiredSessions(0); dropped != 0 {
		t.Fatalf("ttl=0 dropped %d entries", dropped)
	}
	if got := r.LastNodeForSession("ws-1", "fresh"); got != "node-b" {
		t.Fatalf("ttl=0 evicted a live key: %q", got)
	}
}

// fakeSessionStore serves one shared soft-affinity row with a fixed remaining
// store lifetime. Reads are counted so a test can tell a local hit from a new
// store read; the embedded nil interface keeps the other methods unused.
type fakeSessionStore struct {
	SharedStore
	row       []byte
	remaining time.Duration
	reads     int
}

func (f *fakeSessionStore) GetSessionNode(context.Context, string, string) (SharedSessionNode, bool, error) {
	f.reads++
	if f.row == nil {
		return SharedSessionNode{}, false, nil
	}
	return SharedSessionNode{Raw: f.row, Remaining: f.remaining}, true, nil
}

// TestPruneExpiredSessionsUsesStoreRemaining pins the local deadline of a row
// read from the shared index: this process's now plus the store-side remaining
// lifetime, never the writer's learned_at. A 10s remainder is retained at
// now+9s (from the cache, no new read) and dropped at now+11s.
func TestPruneExpiredSessionsUsesStoreRemaining(t *testing.T) {
	now := testNow
	raw, err := json.Marshal(sessionNodeRow{
		WorkspaceID: "ws-1",
		Key:         "sess-1",
		Node:        "node-a",
		LearnedAt:   now.Add(-5 * time.Hour),
	})
	if err != nil {
		t.Fatalf("marshal session row: %v", err)
	}
	shared := &fakeSessionStore{row: raw, remaining: 10 * time.Second}
	r := New(func() time.Time { return now })
	r.shared = shared

	if got := r.LastNodeForSession("ws-1", "sess-1"); got != "node-a" {
		t.Fatalf("lookup = %q, want node-a", got)
	}
	if shared.reads != 1 {
		t.Fatalf("store reads after the first lookup = %d, want 1", shared.reads)
	}

	now = now.Add(9 * time.Second)
	if dropped := r.PruneExpiredSessions(time.Hour); dropped != 0 {
		t.Fatalf("dropped %d rows 9s into a 10s store lifetime", dropped)
	}
	if got := r.LastNodeForSession("ws-1", "sess-1"); got != "node-a" {
		t.Fatalf("retained row lookup = %q, want node-a", got)
	}
	if shared.reads != 1 {
		t.Fatalf("retained row fell back to the store (%d reads)", shared.reads)
	}

	now = now.Add(2 * time.Second) // one second past the store's remainder
	if dropped := r.PruneExpiredSessions(time.Hour); dropped != 1 {
		t.Fatalf("dropped = %d, want 1 once the store lifetime ended", dropped)
	}
}
