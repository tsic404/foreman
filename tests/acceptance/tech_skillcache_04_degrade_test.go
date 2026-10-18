// TC-tech-skillcache-04 / AC-62 (bounded downgrade): when the upstream is
// unreachable, times out or answers 5xx, an entry still inside the downgrade
// window answers instead — byte-identical to the entry, with an error-level
// event and a degraded count. Past the window (or with no entry) the upstream's
// failure returns unchanged. A zero window turns the downgrade off without
// touching the fresh-window hit.
package acceptance

import (
	"bytes"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
)

func TestSkillCache04UpstreamFailureDegradesInsideTheWindow(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	h.upstream.setSkillAnswer(200, `{"bundles":[{"name":"bundle-e","variant":1}]}`)
	path, token := skillTask(t, h, "task-1")
	const payload = `{"refs":["skill-e@1"]}`

	code, first := resolveSkill(t, h, path, token, payload)
	if code != 200 {
		t.Fatalf("first S26 = %d (%s), want 200", code, first)
	}

	// The upstream starts failing; the entry is past its fresh window but still
	// inside the downgrade window.
	h.upstream.setSkillAnswer(503, `{"error":"upstream down"}`)
	h.skillClock.advance(skillFreshTTL + time.Minute)
	code, degraded := resolveSkill(t, h, path, token, payload)
	if code != 200 || !bytes.Equal(degraded, first) {
		t.Fatalf("degraded answer = %d %s, want the entry's body verbatim", code, degraded)
	}
	if n := h.metrics.skillResultCount(proxy.SkillResultDegraded); n != 1 {
		t.Errorf("degraded count = %d, want 1", n)
	}
	// The degraded answer is never silent: an error-level event names the
	// request, and the refresh attempt did reach upstream.
	events := h.handlerEvents("skill.cache_degraded")
	if len(events) != 1 {
		t.Fatalf("skill.cache_degraded events = %d, want 1", len(events))
	}
	if got := events[0]["level"]; got != "ERROR" {
		t.Errorf("degraded event level = %v, want ERROR", got)
	}
	if got := events[0]["task_id"]; got != "task-1" {
		t.Errorf("degraded event task_id = %v, want task-1", got)
	}
	if n := len(h.upstream.calls("POST", "/api/daemon/runtimes/rid-zz/tasks/task-1/skill-bundles/resolve")); n != 2 {
		t.Errorf("C19 calls = %d, want 2 (the downgrade follows a failed refresh attempt)", n)
	}

	// Past the downgrade window the failure returns as it came.
	h.skillClock.advance(skillDowngradeWindow)
	code, failure := resolveSkill(t, h, path, token, payload)
	if code != 503 || string(failure) != `{"error":"upstream down"}` {
		t.Fatalf("out-of-window answer = %d %s, want the upstream's 503 verbatim", code, failure)
	}
	if n := h.metrics.skillResultCount(proxy.SkillResultDegraded); n != 1 {
		t.Errorf("degraded count = %d, want 1 (past the window there is no degraded answer)", n)
	}
	if n := h.metrics.skillResultCount(proxy.SkillResultUpstreamFailure); n != 1 {
		t.Errorf("upstream-failure count = %d, want 1", n)
	}
}

func TestSkillCache04ZeroWindowTurnsTheDowngradeOff(t *testing.T) {
	h := newHarnessWithSkill(t, false, deliveryConfirmWindow, func(c *proxy.Config) {
		c.SkillCacheDowngradeWindow = 0
	})
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	h.upstream.setSkillAnswer(200, `{"bundles":[{"name":"bundle-f","variant":1}]}`)
	path, token := skillTask(t, h, "task-1")
	const payload = `{"refs":["skill-f@1"]}`

	code, first := resolveSkill(t, h, path, token, payload)
	if code != 200 {
		t.Fatalf("first S26 = %d (%s), want 200", code, first)
	}
	// The fresh-window hit is independent of the downgrade switch.
	code, hit := resolveSkill(t, h, path, token, payload)
	if code != 200 || !bytes.Equal(hit, first) {
		t.Fatalf("fresh hit with the window off = %d %s, want the entry verbatim", code, hit)
	}
	if n := h.upstream.skillCalls(); n != 1 {
		t.Fatalf("upstream resolves = %d, want 1 (the fresh hit still answers locally)", n)
	}

	// Past the fresh window with the upstream failing: the failure stands.
	h.upstream.setSkillAnswer(503, `{"error":"upstream down"}`)
	h.skillClock.advance(skillFreshTTL + time.Minute)
	code, failure := resolveSkill(t, h, path, token, payload)
	if code != 503 || string(failure) != `{"error":"upstream down"}` {
		t.Fatalf("zero-window failure = %d %s, want the upstream's 503 verbatim", code, failure)
	}
	if n := h.metrics.skillResultCount(proxy.SkillResultDegraded); n != 0 {
		t.Errorf("degraded count = %d, want 0 with the window off", n)
	}
}
