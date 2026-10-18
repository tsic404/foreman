// TC-tech-skillcache-06 / AC-63 (bounded cache): both capacity bounds hold by
// reclaiming the least recently used entries, the retention reclaims entries no
// request is tracking any more, a reclaimed key is rebuilt on its next resolve,
// and concurrent requests for one key produce exactly one upstream call.
package acceptance

import (
	"bytes"
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
)

func TestSkillCache06EntryBoundReclaimsLRU(t *testing.T) {
	h := newHarnessWithSkill(t, false, deliveryConfirmWindow, func(c *proxy.Config) {
		c.SkillCacheMaxEntries = 2
	})
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	h.upstream.setSkillAnswer(200, `{"bundles":[{"name":"bundle-h"}]}`)
	path, token := skillTask(t, h, "task-1")

	for _, payload := range []string{`{"refs":["h@1"]}`, `{"refs":["h@2"]}`, `{"refs":["h@3"]}`} {
		if code, body := resolveSkill(t, h, path, token, payload); code != 200 {
			t.Fatalf("resolve %s = %d (%s), want 200", payload, code, body)
		}
	}
	if entries, _ := h.metrics.skillSize(); entries != 2 {
		t.Fatalf("cached entries = %d, want the 2-entry bound", entries)
	}
	if n := h.metrics.skillEvictionCount(proxy.SkillEvictCapacity); n != 1 {
		t.Fatalf("capacity reclaims = %d, want 1", n)
	}

	// The least recently used key (h@1) was the one reclaimed: resolving it is a
	// miss that rebuilds the entry.
	before := h.upstream.skillCalls()
	if code, _ := resolveSkill(t, h, path, token, `{"refs":["h@1"]}`); code != 200 {
		t.Fatalf("reclaimed-key resolve = %d, want 200", code)
	}
	if n := h.upstream.skillCalls(); n != before+1 {
		t.Errorf("upstream resolves after the reclaimed key = %d, want %d", n, before+1)
	}
	if entries, _ := h.metrics.skillSize(); entries != 2 {
		t.Errorf("cached entries after the rebuild = %d, want 2", entries)
	}
}

func TestSkillCache06ByteBoundReclaimsLRU(t *testing.T) {
	h := newHarnessWithSkill(t, false, deliveryConfirmWindow, func(c *proxy.Config) {
		c.SkillCacheMaxBytes = 64
	})
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	h.upstream.setSkillAnswer(200, `{"payload":"0123456789012345678901234567890123456789"}`) // 52 bytes
	path, token := skillTask(t, h, "task-1")

	for _, payload := range []string{`{"refs":["i@1"]}`, `{"refs":["i@2"]}`} {
		if code, body := resolveSkill(t, h, path, token, payload); code != 200 {
			t.Fatalf("resolve %s = %d (%s), want 200", payload, code, body)
		}
	}
	entries, bytesHeld := h.metrics.skillSize()
	if bytesHeld > 64 {
		t.Fatalf("cached bytes = %d, want the 64-byte bound", bytesHeld)
	}
	if entries != 1 {
		t.Errorf("cached entries = %d, want 1 (the byte bound reclaimed the first)", entries)
	}
	if n := h.metrics.skillEvictionCount(proxy.SkillEvictCapacity); n != 1 {
		t.Errorf("capacity reclaims = %d, want 1", n)
	}
}

func TestSkillCache06RetentionReclaimsIdleEntries(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	h.upstream.setSkillAnswer(200, `{"bundles":[{"name":"bundle-j"}]}`)
	path, token := skillTask(t, h, "task-1")
	const payload = `{"refs":["j@1"]}`

	if code, _ := resolveSkill(t, h, path, token, payload); code != 200 {
		t.Fatalf("first S26 = %d, want 200", code)
	}
	if entries, _ := h.metrics.skillSize(); entries != 1 {
		t.Fatalf("cached entries = %d, want 1", entries)
	}

	// Nothing uses the entry for the whole retention: the next request reclaims
	// it and rebuilds from upstream.
	h.skillClock.advance(skillRetention + time.Minute)
	before := h.upstream.skillCalls()
	if code, _ := resolveSkill(t, h, path, token, payload); code != 200 {
		t.Fatalf("post-retention resolve = %d, want 200", code)
	}
	if n := h.metrics.skillEvictionCount(proxy.SkillEvictRetention); n != 1 {
		t.Errorf("retention reclaims = %d, want 1", n)
	}
	if n := h.upstream.skillCalls(); n != before+1 {
		t.Errorf("upstream resolves after the retention = %d, want %d", n, before+1)
	}
	if entries, _ := h.metrics.skillSize(); entries != 1 {
		t.Errorf("cached entries after the rebuild = %d, want 1", entries)
	}
}

func TestSkillCache06ConcurrentSameKeyUsesOneUpstreamCall(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	const answer = `{"bundles":[{"name":"bundle-k"}]}`
	h.upstream.setSkillAnswer(200, answer)
	path, token := skillTask(t, h, "task-1")
	const payload = `{"refs":["k@1"]}`

	// The upstream holds its answer until the scenario releases it, so the
	// callers really are concurrent on one key.
	release := h.upstream.gateSkill()
	const callers = 5
	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		failures []string
		bodies   [][]byte
	)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			code, body, err := h.probeCall("POST", path, token, []byte(payload))
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err != nil:
				failures = append(failures, err.Error())
			case code != 200:
				failures = append(failures, "status "+string(body))
			default:
				bodies = append(bodies, body)
			}
		}()
	}
	waitFor(t, "the first upstream resolve", 5*time.Second, func() bool {
		return h.upstream.skillCalls() >= 1
	})
	release()
	wg.Wait()

	if len(failures) != 0 {
		t.Fatalf("concurrent resolves failed: %v", failures)
	}
	if n := h.upstream.skillCalls(); n != 1 {
		t.Errorf("upstream resolves for %d concurrent callers = %d, want 1 (single flight)", callers, n)
	}
	if len(bodies) != callers {
		t.Fatalf("answered callers = %d, want %d", len(bodies), callers)
	}
	for _, body := range bodies {
		if !bytes.Equal(body, []byte(answer)) {
			t.Errorf("concurrent answer = %s, want the upstream body verbatim", body)
		}
	}
}

// The flight's verdict belongs to the key, not to the request that performed
// it: a leader whose own client goes away must not become the waiters' answer —
// neither a 502 on the miss path, nor a fabricated downgrade when a stale entry
// is still inside the window (another task resolving the same reference set is
// the common case, so the waiters are usually other tasks' requests).
func TestSkillCache06WaiterKeepsTheUpstreamVerdictWhenTheLeaderCancels(t *testing.T) {
	for _, stale := range []bool{false, true} {
		name := "miss"
		if stale {
			name = "stale entry inside the window"
		}
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, false)
			h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
			h.declare(declaredRuntimes())
			leaderPath, leaderToken := skillTask(t, h, "task-1")
			waiterPath, waiterToken := skillTask(t, h, "task-2")
			const payload = `{"refs":["k@1"]}`
			const answer = `{"bundles":[{"name":"bundle-k","variant":2}]}`

			h.upstream.setSkillAnswer(200, `{"bundles":[{"name":"bundle-k","variant":1}]}`)
			face := "skill.cache_miss"
			if stale {
				// Land an entry and age it past the fresh window: a leader
				// cancellation must not turn it into a downgrade for the waiter.
				if code, first := resolveSkill(t, h, waiterPath, waiterToken, payload); code != 200 {
					t.Fatalf("seeding resolve = %d (%s), want 200", code, first)
				}
				h.skillClock.advance(skillFreshTTL + time.Minute)
				face = "skill.cache_stale"
			}
			h.upstream.setSkillAnswer(200, answer)

			release := h.upstream.gateSkill()
			defer release()

			// The leader goes away while the upstream still holds the answer.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			leader := make(chan error, 1)
			go func() {
				req, err := http.NewRequestWithContext(ctx, "POST", h.jobsHTTP.URL+leaderPath, bytes.NewReader([]byte(payload)))
				if err != nil {
					leader <- err
					return
				}
				req.Header.Set("Authorization", "Bearer "+leaderToken)
				resp, err := http.DefaultClient.Do(req)
				if err == nil {
					resp.Body.Close()
				}
				leader <- err
			}()

			type resolveResult struct {
				code int
				body []byte
				err  error
			}
			waiter := make(chan resolveResult, 1)
			go func() {
				code, body, err := h.probeCall("POST", waiterPath, waiterToken, []byte(payload))
				waiter <- resolveResult{code: code, body: body, err: err}
			}()

			// Both requests share the key once the waiter has logged its own
			// face: the leader's upstream call is still held by the gate, so the
			// waiter has joined the flight rather than started one of its own.
			waitFor(t, "the leader's upstream resolve", 5*time.Second, func() bool {
				return h.upstream.skillCalls() >= 1
			})
			waitFor(t, "the waiter's "+face+" event", 5*time.Second, func() bool {
				return len(h.handlerEvents(face)) >= 2
			})
			cancel()
			<-leader
			release()

			got := <-waiter
			if got.err != nil {
				t.Fatalf("waiter resolve: %v", got.err)
			}
			if got.code != 200 || !bytes.Equal(got.body, []byte(answer)) {
				t.Fatalf("waiter answer = %d %s, want the upstream's own body %s", got.code, got.body, answer)
			}
			if n := h.metrics.skillResultCount(proxy.SkillResultDegraded); n != 0 {
				t.Errorf("degraded count = %d, want 0 (a leader cancellation is not an upstream failure)", n)
			}
			if n := len(h.handlerEvents("skill.cache_degraded")); n != 0 {
				t.Errorf("skill.cache_degraded events = %d, want 0", n)
			}
		})
	}
}
