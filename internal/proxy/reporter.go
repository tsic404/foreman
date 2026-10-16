package proxy

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/tsic404/foreman/internal/scheduler"
)

// codeLabel renders a C21 status code as the metrics label value; an abandoned
// relay is reported as "none" (observability.md 指标口径).
func codeLabel(code int) string { return strconv.Itoa(code) }

// CatalogProbe is the control channel to the catalog-probe Jobs: build/wait/
// delete. The scheduler owns it (K8s objects); proxy only ever calls it and
// shares state with it through the probe session table (§2 CatalogProbe).
type CatalogProbe interface {
	// StartProbe builds the probe Job and its credential Secret;
	// realRuntimeID is empty in startup-discovery mode.
	StartProbe(ctx context.Context, requestID, realRuntimeID string, budget time.Duration) (scheduler.ProbeHandle, error)
	// AwaitProbeRelayed waits until the probe's result has landed (startup
	// discovery) or been forwarded (command driven), or the deadline passes.
	AwaitProbeRelayed(ctx context.Context, h scheduler.ProbeHandle, deadline time.Time) (nodeName string, relayed bool, relayedCode int, err error)
	// DeleteProbe tears the probe down: session first, then Secret, then Job.
	DeleteProbe(ctx context.Context, h scheduler.ProbeHandle) error
}

// ModelCatalogReporter consumes the C2 ack's pending_model_list and answers it
// (§1.1「C2 ack 消费」, C21): a slot with a cache replays it verbatim, an empty
// slot falls back to a fresh probe whose result is forwarded verbatim. Both
// paths are single-flight per real runtime; a replay never occupies the probe
// slot.
type ModelCatalogReporter struct {
	cfg     Config
	client  *Client
	probes  CatalogProbe
	slots   *CatalogSlots
	metrics Metrics
	log     *slog.Logger
	now     func() time.Time

	mu         sync.Mutex
	perRuntime map[string]*runtimeProbeSlot
}

// runtimeProbeSlot is one real runtime's single-flight state: at most one probe
// in flight, plus a one-deep waiting slot (a newer request supersedes it).
type runtimeProbeSlot struct {
	occupied *pendingCatalogRequest
	queued   *pendingCatalogRequest
}

type pendingCatalogRequest struct {
	reqID      string
	consumedAt time.Time
}

// ReporterOption customizes a ModelCatalogReporter.
type ReporterOption func(*ModelCatalogReporter)

// WithReporterClock overrides the clock (tests).
func WithReporterClock(now func() time.Time) ReporterOption {
	return func(r *ModelCatalogReporter) { r.now = now }
}

// WithReporterLogger overrides the logger (tests).
func WithReporterLogger(l *slog.Logger) ReporterOption {
	return func(r *ModelCatalogReporter) { r.log = l.With("component", "model-catalog") }
}

// NewModelCatalogReporter wires the reporter to the server client (C21), the
// probe control channel, the catalog slots and the metrics seam.
func NewModelCatalogReporter(cfg Config, client *Client, probes CatalogProbe, slots *CatalogSlots, metrics Metrics, opts ...ReporterOption) *ModelCatalogReporter {
	r := &ModelCatalogReporter{
		cfg:        cfg,
		client:     client,
		probes:     probes,
		slots:      slots,
		metrics:    orNoopMetrics(metrics),
		log:        slog.Default().With("component", "model-catalog"),
		now:        time.Now,
		perRuntime: map[string]*runtimeProbeSlot{},
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// probeMinBudget is the smallest budget worth starting a probe with: it only
// covers build + command delivery, not completion (implementation constant,
// not a configuration knob).
func probeMinBudget(timeout time.Duration) time.Duration {
	return timeout / 3
}

// OnAck consumes one heartbeat ack. It runs inside the heartbeat round of the
// runtime the ack belongs to (§1.1「C2 ack 消费」第 3 条): an empty slot is
// filled by a fresh probe, a cached slot is replayed right here.
func (r *ModelCatalogReporter) OnAck(ctx context.Context, runtimeID, requestID string) {
	if requestID == "" {
		return
	}
	if !r.cfg.ModelCatalogEnabled {
		// The disabled path is a visible degradation, never a silent drop.
		r.log.Warn("model.catalog_relay_failed", "request_id", requestID,
			"runtime_id", runtimeID, "reason", "catalog relay disabled")
		r.metrics.ModelCatalogRelay("none", 0)
		return
	}
	if runtimeID == "" {
		// A command always belongs to a real runtime (§1.1「C2 ack 消费」第 1
		// 条). Without one the probe would be built in startup-discovery mode,
		// whose result is absorbed locally and never relayed: the command would
		// die as an upstream window timeout while the relay metric reported a
		// success that never reached the server. Fail visibly instead.
		r.log.Warn("model.catalog_relay_failed", "request_id", requestID,
			"runtime_id", runtimeID, "reason", "no real runtime for the command")
		r.metrics.ModelCatalogRelay("none", 0)
		return
	}
	if slot, ok := r.slots.Get(runtimeID); ok {
		// The answer leaves the heartbeat round: consuming the ack stays
		// in-round (§1.1 C2 ack 消费 第 3 条), while the replay — which may
		// retry inside its own budget — must not stretch the 15s cadence of
		// this runtime's C2 stream (a replay is meant to be the near-zero path).
		go r.replay(context.WithoutCancel(ctx), runtimeID, requestID, slot)
		return
	}
	r.Submit(ctx, runtimeID, requestID, r.now())
}

// replay answers a command from the runtime's catalog cache: the body and the
// request id go out byte for byte, and the slot is not single-flight state
// (§1.1 C21 第 13 条).
func (r *ModelCatalogReporter) replay(ctx context.Context, runtimeID, requestID string, slot CatalogSlot) {
	start := r.now()
	age := start.Sub(slot.LandedAt)
	if r.cfg.ModelCatalogCacheTTL > 0 && age > r.cfg.ModelCatalogCacheTTL {
		// Staleness is only ever a warning: crossing the TTL must never stop
		// the answer (§5.1 目录缓存 TTL).
		r.log.Warn("model.catalog_stale", "request_id", requestID, "runtime_id", runtimeID,
			"age_seconds", age.Seconds(), "ttl_seconds", r.cfg.ModelCatalogCacheTTL.Seconds())
	}
	// The replay is a C21 like any other: transport errors and 5xx retry with
	// the §1.1 C21 backoff inside the same local budget the probe path uses.
	budget := start.Add(r.cfg.ModelCatalogTimeout)
	relayCtx, cancel := context.WithDeadline(ctx, budget)
	defer cancel()
	code, _, err := relayCatalog(relayCtx, r.client, runtimeID, requestID, slot.Body, budget, r.log)
	elapsed := r.now().Sub(start).Seconds()
	if err != nil {
		r.log.Warn("model.catalog_relay_failed", "request_id", requestID, "runtime_id", runtimeID, "reason", err)
		r.metrics.ModelCatalogRelay("none", elapsed)
		return
	}
	r.log.Info("model.catalog_relayed", "request_id", requestID, "code", code)
	r.metrics.ModelCatalogRelay(codeLabel(code), elapsed)
}

// Submit enqueues one command for its runtime. The occupied/queued transition
// and the superseded warning are a single atomic step against release, so a
// request logged as superseded is never also started (§1.1 C21 第 13 条).
func (r *ModelCatalogReporter) Submit(ctx context.Context, runtimeID, requestID string, consumedAt time.Time) {
	req := &pendingCatalogRequest{reqID: requestID, consumedAt: consumedAt}
	start := (*pendingCatalogRequest)(nil)
	r.mu.Lock()
	slot := r.perRuntime[runtimeID]
	if slot == nil {
		slot = &runtimeProbeSlot{}
		r.perRuntime[runtimeID] = slot
	}
	switch {
	case slot.occupied != nil && slot.occupied.reqID == requestID:
	case slot.queued != nil && slot.queued.reqID == requestID:
	case slot.occupied == nil:
		slot.occupied = req
		start = req
	default:
		if slot.queued != nil {
			r.log.Warn("model.catalog_relay_failed", "request_id", slot.queued.reqID,
				"runtime_id", runtimeID, "reason", "superseded by a newer request")
			r.metrics.ModelCatalogRelay("none", 0)
		}
		slot.queued = req
	}
	r.mu.Unlock()
	if start != nil {
		go r.run(context.WithoutCancel(ctx), runtimeID, start)
	}
}

// release frees the occupancy and promotes the waiting request, if any. It is
// the only place a queued request can start.
func (r *ModelCatalogReporter) release(runtimeID string) {
	var next *pendingCatalogRequest
	r.mu.Lock()
	if slot := r.perRuntime[runtimeID]; slot != nil {
		next = slot.queued
		slot.occupied = next
		slot.queued = nil
	}
	r.mu.Unlock()
	if next != nil {
		go r.run(context.Background(), runtimeID, next)
	}
}

// run drives one command through a fresh probe. The budget is anchored at the
// moment the ack was consumed — never at dequeue time: the server window starts
// at delivery, and a queued request may already have waited out one full probe
// (§1.1 C21 第 6 条).
func (r *ModelCatalogReporter) run(ctx context.Context, runtimeID string, req *pendingCatalogRequest) {
	start := r.now()
	// LIFO: Delete must finish before release promotes the next request, or two
	// probe Jobs of the same runtime coexist.
	defer r.release(runtimeID)
	deadline := req.consumedAt.Add(r.cfg.ModelCatalogTimeout)
	remaining := deadline.Sub(start)
	if remaining < probeMinBudget(r.cfg.ModelCatalogTimeout) {
		r.log.Warn("model.catalog_relay_failed", "request_id", req.reqID, "runtime_id", runtimeID,
			"reason", "insufficient budget after queueing")
		r.metrics.ModelCatalogRelay("none", start.Sub(req.consumedAt).Seconds())
		return
	}
	r.log.Info("model.catalog_requested", "request_id", req.reqID, "remaining_ms", remaining.Milliseconds())
	probe, err := r.probes.StartProbe(ctx, req.reqID, runtimeID, remaining)
	if err != nil {
		r.log.Warn("model.catalog_relay_failed", "request_id", req.reqID, "runtime_id", runtimeID, "reason", err)
		r.metrics.ModelCatalogRelay("none", r.now().Sub(req.consumedAt).Seconds())
		return
	}
	defer r.probes.DeleteProbe(context.WithoutCancel(ctx), probe)
	node, relayed, code, err := r.probes.AwaitProbeRelayed(ctx, probe, deadline)
	if err != nil || !relayed {
		r.log.Warn("model.catalog_relay_failed", "request_id", req.reqID, "runtime_id", runtimeID,
			"node_name", node, "reason", reasonOf(err))
		r.metrics.ModelCatalogRelay("none", r.now().Sub(req.consumedAt).Seconds())
		return
	}
	if code == 0 {
		// RelayedAt with code 0: the S27 path used up its retry budget without
		// the server taking the result — the "转发耗尽" face of the failure set.
		r.log.Warn("model.catalog_relay_failed", "request_id", req.reqID, "runtime_id", runtimeID,
			"node_name", node, "reason", errRelayExhausted)
		r.metrics.ModelCatalogRelay("none", r.now().Sub(req.consumedAt).Seconds())
		return
	}
	r.log.Info("model.catalog_relayed", "request_id", req.reqID, "node_name", node, "code", code)
	r.metrics.ModelCatalogRelay(codeLabel(code), r.now().Sub(req.consumedAt).Seconds())
}

// reasonOf keeps the failure event readable when the wait simply ran out.
func reasonOf(err error) any {
	if err == nil {
		return "probe did not report within budget"
	}
	return err
}
