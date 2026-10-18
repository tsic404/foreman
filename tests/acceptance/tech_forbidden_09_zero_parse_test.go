// TC-tech-forbidden-09 / AC-14 (F10 zero-parse negative): Foreman never
// interprets the resolve payload or the upstream body. Three checks must all
// come back empty — no log line or metric carries a payload field name or skill
// content, the hit answer is byte-identical to the upstream answer, and a
// non-canonical payload (key order, whitespace, unknown fields) is forwarded
// byte for byte and its answer replayed byte for byte. The one carve-out is the
// integrity digest over Foreman's own stored bytes (§1.8 零解析).
package acceptance

import (
	"bytes"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/proxy"
)

// skillBodyMarkers are payload field names plus skill content: a Foreman that
// parsed either would leave one of them in a log line or a metric.
var skillBodyMarkers = []string{"unknown_field", "payload_field_marker", "skill_content_marker", "bundle_z"}

func TestForbidden09SkillResolveZeroParse(t *testing.T) {
	const (
		payload = `{ "refs" : ["skill-z@1"], "unknown_field" : "payload_field_marker" }`
		answer  = `{"bundles":[{"name":"bundle_z","content":"skill_content_marker"}]}`
	)

	// The S26 path: a miss forwards the payload verbatim, the hit replays the
	// upstream body byte for byte.
	h := newHarness(t, false)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	h.upstream.setSkillAnswer(200, answer)
	path, token := skillTask(t, h, "task-1")

	code, miss := resolveSkill(t, h, path, token, payload)
	if code != 200 || string(miss) != answer {
		t.Fatalf("miss = %d %s, want the upstream body verbatim", code, miss)
	}
	calls := h.upstream.calls("POST", "/api/daemon/runtimes/rid-zz/tasks/task-1/skill-bundles/resolve")
	if len(calls) != 1 {
		t.Fatalf("C19 calls = %d, want 1", len(calls))
	}
	if !bytes.Equal(calls[0].Body, []byte(payload)) {
		t.Errorf("forwarded body = %s, want the non-canonical payload bytes verbatim", calls[0].Body)
	}
	code, hit := resolveSkill(t, h, path, token, payload)
	if code != 200 || string(hit) != answer {
		t.Fatalf("hit = %d %s, want the cached body verbatim", code, hit)
	}
	if n := len(h.upstream.calls("POST", "/api/daemon/runtimes/rid-zz/tasks/task-1/skill-bundles/resolve")); n != 1 {
		t.Errorf("C19 calls after the hit = %d, want 1", n)
	}

	// The production metric implementation: every series of the cache families
	// is labelled by the outcome or the reason alone, and the exposition carries
	// no payload byte.
	metrics := observability.NewMetrics()
	cacheLogs := &writeBuffer{}
	clock := &steppedClock{}
	cache := proxy.NewSkillCache(h.cfg, h.client, metrics,
		proxy.WithSkillCacheClock(clock.now),
		proxy.WithSkillCacheLogger(slog.New(slog.NewJSONHandler(cacheLogs, nil))))
	if code, body, err := cache.Resolve(t.Context(), "task-1", "rid-job", []byte(payload)); err != nil || code != 200 || string(body) != answer {
		t.Fatalf("direct miss = %d %s (%v), want the upstream body verbatim", code, body, err)
	}
	if code, body, err := cache.Resolve(t.Context(), "task-1", "rid-job", []byte(payload)); err != nil || code != 200 || string(body) != answer {
		t.Fatalf("direct hit = %d %s (%v), want the cached body verbatim", code, body, err)
	}
	// The degraded answer is the one face that logs at error level; it must not
	// leak the body either.
	h.upstream.setSkillAnswer(503, `{"error":"upstream down"}`)
	clock.advance(skillFreshTTL + time.Minute)
	if code, body, err := cache.Resolve(t.Context(), "task-1", "rid-job", []byte(payload)); err != nil || code != 200 || string(body) != answer {
		t.Fatalf("direct degraded answer = %d %s (%v), want the cached body verbatim", code, body, err)
	}

	exposition := scrapeMetrics(t, metrics)
	for family, want := range map[string][]string{
		"foreman_skill_cache_resolves_total":  {"result"},
		"foreman_skill_cache_evictions_total": {"reason"},
		"foreman_skill_cache_entries":         nil,
		"foreman_skill_cache_bytes":           nil,
	} {
		for _, labels := range seriesLabelNames(t, exposition, family) {
			if !slices.Equal(labels, want) {
				t.Errorf("%s series carries labels %v, want %v", family, labels, want)
			}
		}
	}
	if !strings.Contains(exposition, `foreman_skill_cache_resolves_total{result="degraded"} 1`) {
		t.Errorf("the degraded face is missing from the exposition: %s", exposition)
	}
	for _, marker := range skillBodyMarkers {
		if strings.Contains(exposition, marker) {
			t.Errorf("the metrics exposition carries %q", marker)
		}
	}

	// No log line of either path carries the payload or its content.
	logs := h.handlerLogs.String() + h.logs.String() + cacheLogs.String()
	for _, marker := range skillBodyMarkers {
		if strings.Contains(logs, marker) {
			t.Errorf("a Foreman log line carries %q: %s", marker, logs)
		}
	}
}
