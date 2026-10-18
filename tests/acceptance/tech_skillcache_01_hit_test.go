// TC-tech-skillcache-01 / AC-60 (hit answers locally): a payload resolved once
// is answered from the process-local cache for every later request inside the
// fresh window — no upstream call, a byte-identical body, and the hit counted.
// The key is the request payload bytes alone, so the same payload resolved by
// another task (another job runtime id in the path) hits too.
package acceptance

import (
	"bytes"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

// skillTask claims one task and registers its Job daemon, returning the
// Job-side resolve path that daemon would call and the token it carries.
func skillTask(t *testing.T, h *harness, taskID string) (path, token string) {
	t.Helper()
	if err := h.sched.OnClaim(t.Context(), deliveryPayload(taskID)); err != nil {
		t.Fatalf("OnClaim(%s): %v", taskID, err)
	}
	token = issueJobToken(t, h, taskID)
	registerTaskDaemon(t, h, taskID, token)
	entry, ok := h.reg.Get(taskID)
	if !ok {
		t.Fatalf("entry %s missing after the claim", taskID)
	}
	return "/api/daemon/runtimes/" + entry.JobRuntimeID + "/tasks/" + taskID + "/skill-bundles/resolve", token
}

// resolveSkill performs one S26 resolve with exactly these payload bytes.
func resolveSkill(t *testing.T, h *harness, path, token, payload string) (int, []byte) {
	t.Helper()
	return h.call("POST", path, token, []byte(payload))
}

// completeSkillTask closes the task normally: the cache scenarios assert the
// delivery semantics are untouched, so the task must still succeed.
func completeSkillTask(t *testing.T, h *harness, taskID, token string) {
	t.Helper()
	if code, body := h.call("POST", "/api/daemon/tasks/"+taskID+"/complete", token, []byte(`{"exit_code":0}`)); code != 200 {
		t.Fatalf("S13 = %d (%s), want 200", code, body)
	}
	if settled := waitTerminal(t, h, taskID); settled.Result != registry.ResultCompleted {
		t.Fatalf("task %s = %s/%s, want completed", taskID, settled.State, settled.Result)
	}
}

func TestSkillCache01FreshHitAnswersWithoutUpstream(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	h.upstream.setSkillAnswer(200, `{"bundles":[{"name":"bundle-a","content":"bytes"}]}`)

	path, token := skillTask(t, h, "task-1")
	const payload = `{"refs":["skill-a@1"]}`

	code, first := resolveSkill(t, h, path, token, payload)
	if code != 200 {
		t.Fatalf("first S26 = %d (%s), want 200", code, first)
	}
	if n := h.upstream.skillCalls(); n != 1 {
		t.Fatalf("upstream resolves after the first request = %d, want 1 (the miss forwards once)", n)
	}

	// Inside the fresh window every later request is answered locally.
	for i := range 3 {
		code, body := resolveSkill(t, h, path, token, payload)
		if code != 200 || !bytes.Equal(body, first) {
			t.Fatalf("hit %d = %d %s, want 200 with the first body verbatim", i+1, code, body)
		}
	}
	if n := h.upstream.skillCalls(); n != 1 {
		t.Errorf("upstream resolves after the hits = %d, want 1 (a hit never touches upstream)", n)
	}
	if n := h.metrics.skillResultCount(proxy.SkillResultHit); n != 3 {
		t.Errorf("hit count = %d, want 3", n)
	}
	if n := h.metrics.skillResultCount(proxy.SkillResultMiss); n != 1 {
		t.Errorf("miss count = %d, want 1", n)
	}

	// Another task — a different job runtime id in the path — resolving the
	// same payload is the same key: a hit, not a second upstream call.
	otherPath, otherToken := skillTask(t, h, "task-2")
	code, body := resolveSkill(t, h, otherPath, otherToken, payload)
	if code != 200 || !bytes.Equal(body, first) {
		t.Fatalf("cross-task resolve = %d %s, want the cached body verbatim", code, body)
	}
	if n := h.upstream.skillCalls(); n != 1 {
		t.Errorf("upstream resolves after the cross-task hit = %d, want 1", n)
	}

	// The hit event locates the request: the second hit names its own task.
	hits := h.handlerEvents("skill.cache_hit")
	if len(hits) != 4 {
		t.Fatalf("skill.cache_hit events = %d, want 4", len(hits))
	}
	if got := hits[len(hits)-1]["task_id"]; got != "task-2" {
		t.Errorf("last hit event task_id = %v, want task-2", got)
	}
	if got := hits[0]["task_id"]; got != "task-1" {
		t.Errorf("first hit event task_id = %v, want task-1", got)
	}
	// The miss is an event too, and it names the request.
	misses := h.handlerEvents("skill.cache_miss")
	if len(misses) != 1 {
		t.Fatalf("skill.cache_miss events = %d, want 1", len(misses))
	}
	if got := misses[0]["task_id"]; got != "task-1" {
		t.Errorf("miss event task_id = %v, want task-1", got)
	}

	// The cache holds exactly the one entry the miss stored.
	if entries, _ := h.metrics.skillSize(); entries != 1 {
		t.Errorf("cached entries = %d, want 1", entries)
	}

	completeSkillTask(t, h, "task-1", token)
}

// TestSkillCache01HitSurvivesTheFreshWindowBoundary pins the window's edge: an
// entry younger than the fresh window is a hit, and one past it is not.
func TestSkillCache01HitSurvivesTheFreshWindowBoundary(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	h.upstream.setSkillAnswer(200, `{"bundles":[{"name":"bundle-b"}]}`)
	path, token := skillTask(t, h, "task-1")
	const payload = `{"refs":["skill-b@1"]}`

	if code, _ := resolveSkill(t, h, path, token, payload); code != 200 {
		t.Fatalf("first S26 = %d, want 200", code)
	}
	// Just inside the window: still a hit.
	h.skillClock.advance(skillFreshTTL - time.Second)
	if code, _ := resolveSkill(t, h, path, token, payload); code != 200 {
		t.Fatalf("in-window S26 = %d, want 200", code)
	}
	if n := h.upstream.skillCalls(); n != 1 {
		t.Fatalf("upstream resolves inside the window = %d, want 1", n)
	}
	// Past the window: a refresh, never a silent hit.
	h.skillClock.advance(2 * time.Second)
	if code, _ := resolveSkill(t, h, path, token, payload); code != 200 {
		t.Fatalf("past-window S26 = %d, want 200", code)
	}
	if n := h.upstream.skillCalls(); n != 2 {
		t.Errorf("upstream resolves past the window = %d, want 2 (the entry is refreshed)", n)
	}
	if n := h.metrics.skillResultCount(proxy.SkillResultStaleHit); n != 1 {
		t.Errorf("stale-hit count = %d, want 1", n)
	}
}
