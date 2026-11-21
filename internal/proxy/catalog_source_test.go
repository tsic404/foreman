package proxy

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeSource is the catalog source seam as the reporter sees it.
type fakeSource struct {
	mu     sync.Mutex
	calls  []CatalogRequest
	answer CatalogAnswer
	err    error
}

func (f *fakeSource) Catalog(_ context.Context, request CatalogRequest) (CatalogAnswer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, request)
	return f.answer, f.err
}

func (f *fakeSource) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func newSourceReporter(t *testing.T, h *reporterHarness, cfg Config, source CatalogSource, opts ...ReporterOption) {
	t.Helper()
	opts = append(opts,
		WithReporterClock(func() time.Time { return h.now }),
		WithReporterLogger(slog.New(slog.NewTextHandler(h.logs, nil))),
	)
	h.reporter = NewModelCatalogReporter(cfg, h.client, source, h.slots, h.metrics, opts...)
}

func TestReporterRelaysDiscoveryBody(t *testing.T) {
	cfg := testConfig("http://unused")
	h := newReporterHarness(t, cfg)
	body := []byte(`{"provider":"omp","models":[{"id":"m1"}]}`)
	source := &fakeSource{answer: CatalogAnswer{Body: body}}
	newSourceReporter(t, h, cfg, source)
	calls := h.recordC21(t, 200)

	h.reporter.OnAck(t.Context(), "rid-a", "req-1")
	waitForCondition(t, "the discovery body to reach the server", func() bool { return calls.len() == 1 })

	if got := string(calls.snapshot()[0].Body); got != string(body) {
		t.Errorf("relayed body = %s, want the discovery body", got)
	}
	if got := h.metrics.catalogCodeCount("200"); got != 1 {
		t.Errorf("relay metric code 200 = %d, want 1", got)
	}
	if !strings.Contains(h.logs.String(), "model.catalog_relayed") {
		t.Errorf("relayed event missing: %s", h.logs.String())
	}
}

func TestReporterDoesNotRelayDiscoveryFailure(t *testing.T) {
	cfg := testConfig("http://unused")
	h := newReporterHarness(t, cfg)
	source := &fakeSource{err: errors.New("model discovery is unsupported: codex app-server is not verified")}
	newSourceReporter(t, h, cfg, source)
	calls := h.recordC21(t, 200)

	h.reporter.OnAck(t.Context(), "rid-a", "req-1")
	waitForCondition(t, "the failure to be recorded", func() bool {
		return h.metrics.catalogCodeCount("none") == 1
	})

	if calls.len() != 0 {
		t.Fatalf("C21 calls = %d, want none for a failed discovery", calls.len())
	}
	if logs := h.logs.String(); !strings.Contains(logs, "model.catalog_relay_failed") ||
		!strings.Contains(logs, "unsupported") {
		t.Errorf("the discovery failure must be visible: %s", logs)
	}
}

func TestReporterDiscoveryBypassesTheSlotCache(t *testing.T) {
	cfg := testConfig("http://unused")
	h := newReporterHarness(t, cfg)
	h.slots.Put("rid-a", []byte(`{"models":[{"id":"stale"}]}`), h.now)
	fresh := []byte(`{"models":[{"id":"fresh"}]}`)
	source := &fakeSource{answer: CatalogAnswer{Body: fresh}}
	newSourceReporter(t, h, cfg, source, WithCatalogSlotReplay(false))
	calls := h.recordC21(t, 200)

	h.reporter.OnAck(t.Context(), "rid-a", "req-1")
	waitForCondition(t, "the source body to answer", func() bool { return calls.len() == 1 })

	if got := string(calls.snapshot()[0].Body); got != string(fresh) {
		t.Errorf("relayed body = %s, want the source body instead of the stale slot", got)
	}
	if source.count() != 1 {
		t.Errorf("source calls = %d, want the slot replay bypassed", source.count())
	}
}

func TestReporterSourceThatAlreadyRelayedDoesNotDoubleRelay(t *testing.T) {
	cfg := testConfig("http://unused")
	h := newReporterHarness(t, cfg)
	source := &fakeSource{answer: CatalogAnswer{Relayed: true, Code: 200, Node: "node-a"}}
	newSourceReporter(t, h, cfg, source)
	calls := h.recordC21(t, 200)

	h.reporter.OnAck(t.Context(), "rid-a", "req-1")
	waitForCondition(t, "the relayed answer to be recorded", func() bool {
		return h.metrics.catalogCodeCount("200") == 1
	})

	if calls.len() != 0 {
		t.Fatalf("C21 calls = %d, want none when the source already relayed", calls.len())
	}
	if !strings.Contains(h.logs.String(), "model.catalog_relayed") {
		t.Errorf("relayed event missing: %s", h.logs.String())
	}
}
