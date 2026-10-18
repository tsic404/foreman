// TC-tech-skillcache-03 / AC-61 (miss forwarding and refresh): the first
// resolve forwards the payload verbatim as C19 and returns the upstream body
// unchanged; an entry past the fresh window is refreshed upstream and replaced,
// so the next request hits again. The miss path's error semantics stay the
// upstream's own — a failure is never turned into a cached success.
package acceptance

import (
	"bytes"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
)

func TestSkillCache03MissForwardsAndRefreshReplaces(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	h.upstream.setSkillAnswer(200, `{"bundles":[{"name":"bundle-d","variant":1}]}`)
	path, token := skillTask(t, h, "task-1")
	const payload = `{"refs":["skill-d@1"]}`

	// First resolve: forwarded with the exact bytes, answered with the upstream
	// body unchanged.
	code, first := resolveSkill(t, h, path, token, payload)
	if code != 200 || string(first) != `{"bundles":[{"name":"bundle-d","variant":1}]}` {
		t.Fatalf("first S26 = %d %s, want the upstream body", code, first)
	}
	// The upstream call carries the real runtime id (not the job runtime id) and
	// the payload bytes verbatim.
	forwarded := h.upstream.calls("POST", "/api/daemon/runtimes/rid-zz/tasks/task-1/skill-bundles/resolve")
	if len(forwarded) != 1 {
		t.Fatalf("C19 calls = %d, want 1 (the path must carry the real runtime id)", len(forwarded))
	}
	if !bytes.Equal(forwarded[0].Body, []byte(payload)) {
		t.Errorf("C19 body = %s, want the payload bytes verbatim", forwarded[0].Body)
	}

	// Past the fresh window: the entry is refreshed, not served.
	h.upstream.setSkillAnswer(200, `{"bundles":[{"name":"bundle-d","variant":2}]}`)
	h.skillClock.advance(skillFreshTTL + time.Minute)
	code, refreshed := resolveSkill(t, h, path, token, payload)
	if code != 200 || string(refreshed) != `{"bundles":[{"name":"bundle-d","variant":2}]}` {
		t.Fatalf("refresh S26 = %d %s, want the new upstream body", code, refreshed)
	}
	if n := len(h.upstream.calls("POST", "/api/daemon/runtimes/rid-zz/tasks/task-1/skill-bundles/resolve")); n != 2 {
		t.Errorf("C19 calls after the refresh = %d, want 2", n)
	}
	if n := h.metrics.skillResultCount(proxy.SkillResultStaleHit); n != 1 {
		t.Errorf("stale-hit count = %d, want 1", n)
	}

	// The replacement answers the next request without touching upstream.
	code, hit := resolveSkill(t, h, path, token, payload)
	if code != 200 || !bytes.Equal(hit, refreshed) {
		t.Fatalf("post-refresh hit = %d %s, want the refreshed body verbatim", code, hit)
	}
	if n := len(h.upstream.calls("POST", "/api/daemon/runtimes/rid-zz/tasks/task-1/skill-bundles/resolve")); n != 2 {
		t.Errorf("C19 calls after the hit = %d, want 2", n)
	}
	if n := h.metrics.skillResultCount(proxy.SkillResultHit); n != 1 {
		t.Errorf("hit count = %d, want 1", n)
	}

	// A payload never cached takes the upstream's failure as it came: no
	// fabricated success, no entry stored.
	h.upstream.setSkillAnswer(503, `{"error":"upstream down"}`)
	code, failure := resolveSkill(t, h, path, token, `{"refs":["skill-d@never"]}`)
	if code != 503 || string(failure) != `{"error":"upstream down"}` {
		t.Fatalf("miss-path failure = %d %s, want the upstream's 503 body verbatim", code, failure)
	}
	if n := h.metrics.skillResultCount(proxy.SkillResultUpstreamFailure); n != 1 {
		t.Errorf("upstream-failure count = %d, want 1", n)
	}
	if entries, _ := h.metrics.skillSize(); entries != 1 {
		t.Errorf("cached entries = %d, want 1 (a failure is never stored)", entries)
	}

	// Each face is its own locatable event: two misses (first resolve, the
	// never-cached failure) and one stale refresh.
	if events := h.handlerEvents("skill.cache_miss"); len(events) != 2 {
		t.Errorf("skill.cache_miss events = %d, want 2", len(events))
	}
	stale := h.handlerEvents("skill.cache_stale")
	if len(stale) != 1 {
		t.Fatalf("skill.cache_stale events = %d, want 1", len(stale))
	}
	if got := stale[0]["task_id"]; got != "task-1" {
		t.Errorf("stale event task_id = %v, want task-1", got)
	}
	if events := h.handlerEvents("skill.cache_upstream_failure"); len(events) != 1 {
		t.Errorf("skill.cache_upstream_failure events = %d, want 1", len(events))
	}
}
