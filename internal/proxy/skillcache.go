package proxy

import (
	"container/list"
	"context"
	"crypto/sha256"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Skill-resolve outcomes (§9.2 foreman_skill_cache_resolves_total): the five
// faces one resolve request can end in.
const (
	SkillResultHit             = "hit"
	SkillResultMiss            = "miss"
	SkillResultStaleHit        = "stale_hit"
	SkillResultDegraded        = "degraded"
	SkillResultUpstreamFailure = "upstream_failure"
)

// Reclaim reasons (§9.2 回收计数): the two capacity bounds, the retention, and
// the integrity check's own verdict.
const (
	SkillEvictCapacity  = "capacity"
	SkillEvictRetention = "retention"
	SkillEvictDamaged   = "damaged"
)

// SkillKey is one resolve's cache key: the digest of the raw request payload.
// The payload is never interpreted, and neither the task id nor the runtime id
// enters the key (§1.8「缓存键 = 请求载荷字节的函数」).
type SkillKey [sha256.Size]byte

// SkillKeyOf hashes a request payload into its cache key.
func SkillKeyOf(payload []byte) SkillKey { return SkillKey(sha256.Sum256(payload)) }

// skillEntry is one stored resolve response: the upstream body verbatim, the
// digest taken when it landed, and the moments the lifecycle rules read.
type skillEntry struct {
	body     []byte
	digest   [sha256.Size]byte
	landedAt time.Time
	usedAt   time.Time
	elem     *list.Element
}

// skillRead is the cache's answer to one lookup.
type skillRead int

const (
	// skillMiss is no entry, or none the integrity check vouches for.
	skillMiss skillRead = iota
	// skillFresh is an entry inside the fresh window: it answers locally.
	skillFresh
	// skillStale is an entry past the fresh window: it must be refreshed, and
	// may only answer again through the downgrade (§1.8).
	skillStale
)

// skillFlight is one key's in-flight upstream call; every waiter reads its
// result after done closes (§1.8 单飞).
type skillFlight struct {
	done chan struct{}
	code int
	body []byte
	err  error
}

// SkillCache is the process-local resolve cache and the §1.8 decision face: a
// fresh entry answers without touching upstream, a miss or a stale entry goes
// upstream once per key, and an upstream failure may still be answered from an
// entry inside the downgrade window. Nothing survives a restart (ADR-045 决策
// 结果 7), and the payload bytes are never interpreted (F10).
type SkillCache struct {
	cfg     Config
	client  *Client
	metrics Metrics
	log     *slog.Logger
	now     func() time.Time

	// mu guards the store and the in-flight map; no network call is ever made
	// with it held.
	mu      sync.Mutex
	entries map[SkillKey]*skillEntry
	order   *list.List // usage order, least recently used first
	bytes   int64
	flights map[SkillKey]*skillFlight
}

// SkillCacheOption customizes a SkillCache.
type SkillCacheOption func(*SkillCache)

// WithSkillCacheClock overrides the clock (tests age entries with it instead of
// waiting the windows out).
func WithSkillCacheClock(now func() time.Time) SkillCacheOption {
	return func(c *SkillCache) { c.now = now }
}

// WithSkillCacheLogger overrides the logger.
func WithSkillCacheLogger(l *slog.Logger) SkillCacheOption {
	return func(c *SkillCache) { c.log = l.With("component", "skill-cache") }
}

// NewSkillCache wires the resolve cache to the upstream client, the metrics
// seam, and the §1.8 windows and bounds the configuration carries.
func NewSkillCache(cfg Config, client *Client, metrics Metrics, opts ...SkillCacheOption) *SkillCache {
	c := &SkillCache{
		cfg:     cfg,
		client:  client,
		metrics: orNoopMetrics(metrics),
		log:     slog.Default().With("component", "skill-cache"),
		now:     time.Now,
		entries: map[SkillKey]*skillEntry{},
		order:   list.New(),
		flights: map[SkillKey]*skillFlight{},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Resolve answers one S26 resolve call (§1.8): a fresh entry replays locally, a
// miss or a stale entry forwards upstream, and a failed upstream may still be
// answered from an entry inside the downgrade window. A transport failure comes
// back as an error so the caller keeps its own 502 shape.
func (c *SkillCache) Resolve(ctx context.Context, taskID, runtimeID string, payload []byte) (int, []byte, error) {
	key := SkillKeyOf(payload)
	now := c.now()
	entry, read, reclaimed := c.read(key, now)
	c.logReclaims(reclaimed, taskID, runtimeID)

	if read == skillFresh {
		c.count(SkillResultHit)
		c.log.Info("skill.cache_hit", "task_id", taskID, "runtime_id", runtimeID,
			"age_seconds", now.Sub(entry.landedAt).Seconds())
		return http.StatusOK, entry.body, nil
	}
	refresh := SkillResultMiss
	if read == skillStale {
		refresh = SkillResultStaleHit
		c.log.Info("skill.cache_stale", "task_id", taskID, "runtime_id", runtimeID,
			"age_seconds", now.Sub(entry.landedAt).Seconds())
	} else {
		c.log.Info("skill.cache_miss", "task_id", taskID, "runtime_id", runtimeID)
	}

	code, body, err := c.forward(ctx, key, taskID, runtimeID, payload)
	switch {
	case err == nil && code == http.StatusOK:
		c.count(refresh)
		return code, body, nil
	case err != nil || code >= http.StatusInternalServerError:
		// The upstream failed. An entry inside the downgrade window is the one
		// answer the design allows in its place; the error-level line and the
		// degraded count are what keep that face from being silent (§1.8).
		if entry != nil && c.canDegrade(entry, now) {
			c.count(SkillResultDegraded)
			c.log.Error("skill.cache_degraded", "task_id", taskID, "runtime_id", runtimeID,
				"age_seconds", now.Sub(entry.landedAt).Seconds(), "reason", upstreamFailure(err, code))
			return http.StatusOK, entry.body, nil
		}
		c.count(SkillResultUpstreamFailure)
		c.log.Warn("skill.cache_upstream_failure", "task_id", taskID, "runtime_id", runtimeID,
			"reason", upstreamFailure(err, code))
		return code, body, err
	default:
		// The upstream's own non-success answer: forwarded verbatim and never
		// stored, so the miss path's error semantics are unchanged (§1.8).
		c.count(refresh)
		return code, body, nil
	}
}

// Put stores body as the entry for key, taking ownership of the bytes: the
// digest is taken here, so a later change to those bytes makes the entry a miss
// rather than an answer — the compare is over Foreman's own stored bytes and
// never over content (§1.8 零解析, F10). It returns the reasons entries were
// reclaimed for, so the request that triggered them can log them (§9.1).
func (c *SkillCache) Put(key SkillKey, body []byte, at time.Time) []string {
	if int64(len(body)) > c.cfg.SkillCacheMaxBytes {
		// An entry larger than the whole byte budget cannot be stored without
		// breaking the bound, and evicting everything else for it would still
		// break it: skip the entry.
		c.log.Warn("skill.cache_entry_too_large", "bytes", len(body), "max_bytes", c.cfg.SkillCacheMaxBytes)
		return nil
	}
	// The digest covers the whole body, so it is taken before the store lock:
	// hashing inside it would make every other key wait on this entry's bytes.
	digest := sha256.Sum256(body)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dropLocked(key)
	entry := &skillEntry{body: body, digest: digest, landedAt: at, usedAt: at}
	entry.elem = c.order.PushBack(key)
	c.entries[key] = entry
	c.bytes += int64(len(body))
	reclaimed := c.enforceLocked()
	c.publishSizeLocked()
	return reclaimed
}

// Delete removes the entry stored for key. A deleted entry is a miss until the
// next upstream success rebuilds it — the "缓存缺失只降级为未命中" face (§1.8).
func (c *SkillCache) Delete(key SkillKey) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries[key] == nil {
		return false
	}
	c.dropLocked(key)
	c.publishSizeLocked()
	return true
}

// read answers one lookup. It also reclaims what the read itself proves
// reclaimable — an entry past the retention, or one whose bytes no longer match
// their landing digest — and reports the reasons to the caller (§9.1).
func (c *SkillCache) read(key SkillKey, now time.Time) (*skillEntry, skillRead, []string) {
	var reclaimed []string
	for {
		c.mu.Lock()
		reclaimed = append(reclaimed, c.evictExpiredLocked(now)...)
		entry := c.entries[key]
		if entry == nil {
			c.mu.Unlock()
			return nil, skillMiss, reclaimed
		}
		body, digest := entry.body, entry.digest
		c.mu.Unlock()

		// The integrity check F10 carves out compares what Foreman itself wrote
		// against the digest it landed with. It costs the entry's size, so it
		// runs outside the lock and is confirmed against the entry still stored
		// under the key before it decides anything.
		intact := sha256.Sum256(body) == digest

		c.mu.Lock()
		if c.entries[key] != entry {
			// The entry was replaced while its bytes were hashed: the verdict
			// describes the bytes it was taken over, so the lookup restarts
			// against whatever is stored now.
			c.mu.Unlock()
			continue
		}
		if !intact {
			reclaimed = append(reclaimed, c.reclaimLocked(entry, SkillEvictDamaged))
			c.mu.Unlock()
			return nil, skillMiss, reclaimed
		}
		// A lookup of a live entry is itself a use: the retention reclaims
		// entries no request is tracking any more.
		entry.usedAt = now
		c.order.MoveToBack(entry.elem)
		fresh := now.Sub(entry.landedAt) < c.cfg.SkillCacheFreshTTL
		c.mu.Unlock()
		if fresh {
			return entry, skillFresh, reclaimed
		}
		return entry, skillStale, reclaimed
	}
}

// forward runs the upstream call once per key: the first caller performs it and
// stores a success, every waiter reuses that result (§1.8 单飞).
func (c *SkillCache) forward(ctx context.Context, key SkillKey, taskID, runtimeID string, payload []byte) (int, []byte, error) {
	c.mu.Lock()
	if flight, ok := c.flights[key]; ok {
		c.mu.Unlock()
		<-flight.done
		return flight.code, flight.body, flight.err
	}
	flight := &skillFlight{done: make(chan struct{})}
	c.flights[key] = flight
	c.mu.Unlock()

	// The flight's verdict is shared with every waiter, so the upstream call is
	// detached from this request's cancellation: a leader whose own client goes
	// away must not hand its cancellation to the waiters as the key's answer (a
	// 502 for them, or a fabricated downgrade). The client's own control timeout
	// is what bounds the call, so the waiters stay bounded with it.
	code, body, err := c.client.ResolveSkillBundles(context.WithoutCancel(ctx), taskID, payload)
	if err == nil && code == http.StatusOK {
		// Only a success is content-addressed: an error answer is never stored,
		// so a failure can never come back as a cached success (§1.8).
		c.logReclaims(c.Put(key, body, c.now()), taskID, runtimeID)
	}
	flight.code, flight.body, flight.err = code, body, err
	c.mu.Lock()
	delete(c.flights, key)
	c.mu.Unlock()
	close(flight.done)
	return code, body, err
}

// canDegrade reports whether an entry may answer an upstream failure: never at
// a zero window, and never past it (§1.8「降级窗口可配置，取零即关闭」).
func (c *SkillCache) canDegrade(entry *skillEntry, now time.Time) bool {
	if c.cfg.SkillCacheDowngradeWindow <= 0 {
		return false
	}
	return now.Sub(entry.landedAt) <= c.cfg.SkillCacheDowngradeWindow
}

// evictExpiredLocked reclaims every entry left unused past the retention. The
// usage order makes the expired ones a prefix of the list.
func (c *SkillCache) evictExpiredLocked(now time.Time) []string {
	var reclaimed []string
	for front := c.order.Front(); front != nil; {
		entry := c.entries[front.Value.(SkillKey)]
		if entry == nil {
			// Defensive: the usage order and the store never disagree, but a
			// stale element must not wedge the walk.
			c.order.Remove(front)
			front = c.order.Front()
			continue
		}
		if now.Sub(entry.usedAt) <= c.cfg.SkillCacheRetention {
			break
		}
		reclaimed = append(reclaimed, c.reclaimLocked(entry, SkillEvictRetention))
		front = c.order.Front()
	}
	return reclaimed
}

// enforceLocked reclaims least-recently-used entries until both capacity bounds
// hold.
func (c *SkillCache) enforceLocked() []string {
	var reclaimed []string
	for c.order.Len() > c.cfg.SkillCacheMaxEntries || c.bytes > c.cfg.SkillCacheMaxBytes {
		front := c.order.Front()
		if front == nil {
			break
		}
		entry := c.entries[front.Value.(SkillKey)]
		if entry == nil {
			c.order.Remove(front)
			continue
		}
		reclaimed = append(reclaimed, c.reclaimLocked(entry, SkillEvictCapacity))
	}
	return reclaimed
}

// reclaimLocked drops one entry and counts the reclaim, returning the reason so
// the request that triggered it can log it (§9.1).
func (c *SkillCache) reclaimLocked(entry *skillEntry, reason string) string {
	key := entry.elem.Value.(SkillKey)
	c.order.Remove(entry.elem)
	delete(c.entries, key)
	c.bytes -= int64(len(entry.body))
	c.metrics.SkillCacheEviction(reason)
	c.publishSizeLocked()
	return reason
}

// dropLocked removes the entry currently stored for key, if any, without
// counting a reclaim: it is a replacement, not a reclamation.
func (c *SkillCache) dropLocked(key SkillKey) {
	entry := c.entries[key]
	if entry == nil {
		return
	}
	c.order.Remove(entry.elem)
	delete(c.entries, key)
	c.bytes -= int64(len(entry.body))
}

// publishSizeLocked reports the store's size to the metrics seam (§9.2 条目数
// 与字节量).
func (c *SkillCache) publishSizeLocked() {
	c.metrics.SkillCacheSize(len(c.entries), c.bytes)
}

// count records one resolve face.
func (c *SkillCache) count(result string) { c.metrics.SkillCache(result) }

// logReclaims records what one request reclaimed (§9.1): a damaged entry is the
// anomaly the integrity check exists for, the two bounds are normal operation.
func (c *SkillCache) logReclaims(reclaimed []string, taskID, runtimeID string) {
	for _, reason := range reclaimed {
		if reason == SkillEvictDamaged {
			c.log.Warn("skill.cache_entry_damaged", "task_id", taskID, "runtime_id", runtimeID)
			continue
		}
		c.log.Info("skill.cache_evicted", "task_id", taskID, "runtime_id", runtimeID, "reason", reason)
	}
}

// upstreamFailure keeps the failure event readable: a transport error keeps its
// own text, a status answer is named as such.
func upstreamFailure(err error, code int) any {
	if err != nil {
		return err
	}
	return "upstream status " + strconv.Itoa(code)
}
