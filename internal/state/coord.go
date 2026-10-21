package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/tsic404/foreman/internal/registry"
)

// Key namespaces of the stage-two coordination face (ADR-046 决策 1-9;
// 02-contracts.md §0 术语「共享状态」「共享待派发队列」). Every key is
// namespaced per workspace so one workspace's rows, queue, counters and locks
// can never adjudicate another's (F11 多 workspace 口径).
const (
	keyQueue   = "foreman:queue:"       // + ws + ":" + task_id (queue marker)
	keyLock    = "foreman:lock:"        // + ws + ":" + task_id (write lock)
	keyLeader  = "foreman:leader"       // deployment-wide control-plane lease
	keySlotAll = "foreman:slots:global" // cluster-level in-flight counter
	keySlotWS  = "foreman:slots:ws:"    // + ws
	// Pending reservations: a ZSET per level, member = reservation id,
	// score = expiry. The counter equals in-flight rows + live reservations;
	// the set is what lets reconciliation separate the two.
	keySlotResAll = "foreman:slots:res:global"
	keySlotResWS  = "foreman:slots:res:ws:" // + ws
	keyInstance   = "foreman:instance:"     // + instance id (liveness row)
	keyDecl       = "foreman:declaration:"  // + ws (registration identity)
	keyProbe      = "foreman:probe:"        // + probe job name (probe session)
	keySession    = "foreman:session:"      // + ws + ":" + session key (soft-affinity index)
	keyCatalog    = "foreman:catalog:"      // + real runtime id (catalog slot)
	keyJobIndex   = "foreman:jobidx:"       // + job name → ws + "\x00" + task id
)

// Pickup is the outcome of one S-6 delivery pickup against the shared queue
// (02-contracts.md §5). Exactly one of the three outcomes holds:
//
//   - Delivered: this caller won the atomic take and must hand Entry.Payload
//     to the daemon (redelivery when Entry.Deliveries > 1);
//   - Held: the task is committed and its confirmation window is still open —
//     the empty batch that answers a lost response (Entry.ClaimsAfterDelivery
//     carries the bumped count);
//   - neither: nothing is deliverable (unregistered, started, terminal, or no
//     row at all — Entry is zero when the row is missing).
type Pickup struct {
	Entry     registry.TaskEntry
	Delivered bool
	Held      bool
	Missing   bool
	// PrevDeliveredAt and PrevClaims capture the delivery facts the commit
	// replaced: the rearm event reports which unconfirmed commit was released
	// and how many empty batches it had drawn (§4.1 交付后的 claim 计数).
	PrevDeliveredAt time.Time
	PrevClaims      int
}

// CoordStore is the stage-two coordination face: the shared delivery queue,
// the per-task write lock, the two-level in-flight counters, the control-plane
// lease, the instance liveness rows, the registration declaration and the
// probe sessions (ADR-046 决策 1-9). The Redis implementation below is the
// concrete face; a deployment without a shared store never gets one, so the
// capability is detected by type assertion and the stage-one path is
// untouched.
type CoordStore interface {
	// Shared reports that the coordination primitives are available.
	Shared() bool
	// Available reports whether the last observed store operation succeeded;
	// DownFor is the elapsed outage (zero while available).
	Available() bool
	DownFor() time.Duration

	// SetEntry writes the full mapping row (claim time) and its Job index.
	SetEntry(ctx context.Context, ws, taskID string, e registry.TaskEntry) error
	// MergeEntry applies the in-place patch to the stored row.
	MergeEntry(ctx context.Context, ws, taskID string, p registry.EntryPatch) error
	// EnqueueEntry performs the single enqueue: a task keeps one queue row and
	// one mapping row, and a repeated enqueue never overwrites the payload
	// (§5 单次入队). A queue marker whose mapping row was lost is rewritten,
	// which is the self-healing face of a lost row.
	EnqueueEntry(ctx context.Context, ws, taskID string, e registry.TaskEntry) (bool, error)
	// PickupDelivery is the atomic S-6 take: it commits one delivery (or
	// re-delivery after the confirmation window) and returns the updated row.
	PickupDelivery(ctx context.Context, ws, taskID string, window time.Duration) (Pickup, error)
	// ConfirmStart writes the once-only start fact and drops the queue marker.
	ConfirmStart(ctx context.Context, ws, taskID string, at time.Time, advance bool) (registry.TaskEntry, bool, error)
	// CommitDelivering records one delivery commit on the shared row (the
	// stage-one direct path; stage two takes through the queue).
	CommitDelivering(ctx context.Context, ws, taskID string, at time.Time, payload []byte, window time.Duration) (registry.TaskEntry, bool, error)
	// CommitJobCreated records the Job creation timestamp.
	CommitJobCreated(ctx context.Context, ws, taskID string, at time.Time) (registry.TaskEntry, bool, error)
	// CommitDaemonRegistered records the S-4 registration.
	CommitDaemonRegistered(ctx context.Context, ws, taskID string, at time.Time) (registry.TaskEntry, bool, error)
	// CommitTerminal writes the terminal verdict, the settled record and drops
	// the queue marker atomically; first=false when already terminal.
	CommitTerminal(ctx context.Context, ws, taskID string, result registry.Result, reason string, completedAt, drainDeadline time.Time) (registry.TaskEntry, bool, error)
	// CommitDecidedReport records the instance's decided terminal body once.
	CommitDecidedReport(ctx context.Context, ws, taskID string, body []byte) (registry.TaskEntry, bool, error)
	// SettleNoResult clears the verdict of a terminal row (§4.4).
	SettleNoResult(ctx context.Context, ws, taskID string, expected registry.Result) (registry.TaskEntry, bool, error)
	// DeleteEntry removes the live row, its queue marker and its Job index.
	DeleteEntry(ctx context.Context, ws, taskID string) error
	// DropQueue removes the queue marker only.
	DropQueue(ctx context.Context, ws, taskID string) error
	// QueueDepth counts the live queue markers.
	QueueDepth(ctx context.Context) (int64, error)

	// GetEntry reads one live row.
	GetEntry(ctx context.Context, ws, taskID string) (registry.TaskEntry, bool, error)
	// GetEntryByJob resolves a Job name through the shared index.
	GetEntryByJob(ctx context.Context, jobName string) (registry.TaskEntry, bool, error)
	// ListEntries lists every live row.
	ListEntries(ctx context.Context) ([]registry.TaskEntry, error)
	// ListSettled lists every settled record.
	ListSettled(ctx context.Context) ([]registry.TaskEntry, error)
	// ListRevoked lists the revocation set.
	ListRevoked(ctx context.Context) (map[string]time.Time, error)

	// LockTask acquires the per-task write lock (lease-bounded).
	LockTask(ctx context.Context, ws, taskID, owner string, ttl time.Duration) (bool, error)
	// UnlockTask releases the lock only when owner still holds it.
	UnlockTask(ctx context.Context, ws, taskID, owner string) error

	// ReserveSlot reserves one in-flight slot when the counter is below limit
	// (limit <= 0 means no per-workspace cap for the workspace counter). The
	// reservation is tracked with a lease, so the reconciliation can tell a
	// pending claim apart from a leaked count.
	ReserveSlot(ctx context.Context, ws string, limit int) (bool, error)
	// ConsumeSlotReservation converts one reservation into entry ownership:
	// the counter is unchanged, only the pending marker goes away.
	ConsumeSlotReservation(ctx context.Context, ws string) error
	// ReleaseSlot abandons one reservation and returns its slot.
	ReleaseSlot(ctx context.Context, ws string) error
	// ReleaseEntry returns an entry's in-flight slot (terminal or reclaimed
	// row). It never touches the pending reservations.
	ReleaseEntry(ctx context.Context, ws string) error
	// SlotSnapshot atomically purges expired reservations (adjusting the
	// counter) and returns the counter and live reservation count of one
	// snapshot: the reconciliation's read face.
	SlotSnapshot(ctx context.Context, ws string) (count, reservations int64, err error)
	// CompareAndSetSlots writes next only when the counter still equals
	// expected, so a concurrent claim's reservation can never be lost.
	CompareAndSetSlots(ctx context.Context, ws string, expected, next int64) (bool, error)
	// ClearSlotReservations drops every pending reservation of one level.
	// The takeover uses it: the previous holder is gone, so no reservation it
	// left can still become a row (AC-76 未用预留在一轮对账内回收).
	ClearSlotReservations(ctx context.Context, ws string) error
	// SetSlots writes the reconciled counter value.
	SetSlots(ctx context.Context, ws string, n int64) error
	// Slots reads the counter (empty ws = cluster-level).
	Slots(ctx context.Context, ws string) (int64, error)

	// AcquireLeadership takes the control-plane lease when it is free.
	AcquireLeadership(ctx context.Context, owner string, ttl time.Duration) (bool, error)
	// RenewLeadership extends the lease when owner still holds it.
	RenewLeadership(ctx context.Context, owner string, ttl time.Duration) (bool, error)
	// ReleaseLeadership drops the lease when owner still holds it.
	ReleaseLeadership(ctx context.Context, owner string) error
	// LeadershipHolder names the current lease holder ("" when free).
	LeadershipHolder(ctx context.Context) (string, error)

	// HeartbeatInstance refreshes one instance's liveness row.
	HeartbeatInstance(ctx context.Context, id string, ttl time.Duration) error
	// ListInstances lists the live instance ids.
	ListInstances(ctx context.Context) ([]string, error)

	// PutDeclaration stores the registration identity of one workspace.
	PutDeclaration(ctx context.Context, ws string, raw []byte) error
	// GetDeclaration reads the registration identity of one workspace.
	GetDeclaration(ctx context.Context, ws string) ([]byte, bool, error)

	// PutProbeSession stores one probe session row (shared across instances).
	PutProbeSession(ctx context.Context, jobName string, raw []byte, ttl time.Duration) error
	// CASProbeSession replaces one probe session row only when it is still
	// byte-identical to expected (the concurrent-update guard).
	CASProbeSession(ctx context.Context, jobName string, expected, next []byte, ttl time.Duration) (bool, error)
	// GetProbeSession reads one probe session row.
	GetProbeSession(ctx context.Context, jobName string) ([]byte, bool, error)
	// DeleteProbeSession removes one probe session row.
	DeleteProbeSession(ctx context.Context, jobName string) error
	// ListProbeSessions lists every probe session row.
	ListProbeSessions(ctx context.Context) (map[string][]byte, error)

	// PutSessionNode stores one soft-affinity row (the session → node index).
	PutSessionNode(ctx context.Context, ws, key string, raw []byte, ttl time.Duration) error
	// GetSessionNode reads one soft-affinity row.
	GetSessionNode(ctx context.Context, ws, key string) ([]byte, bool, error)
	// DeleteSessionNode removes one soft-affinity row.
	DeleteSessionNode(ctx context.Context, ws, key string) error
	// ListSessionNodes lists every soft-affinity row, keyed by
	// ws + "\x00" + session key.
	ListSessionNodes(ctx context.Context) (map[string][]byte, error)

	// PutCatalogSlot stores one real runtime's catalog slot (§1.3: the read
	// and write may land on different instances).
	PutCatalogSlot(ctx context.Context, runtimeID string, raw []byte, ttl time.Duration) error
	// GetCatalogSlot reads one catalog slot.
	GetCatalogSlot(ctx context.Context, runtimeID string) ([]byte, bool, error)
	// DeleteCatalogSlot removes one catalog slot.
	DeleteCatalogSlot(ctx context.Context, runtimeID string) error
}

var _ CoordStore = (*redisStore)(nil)

// Shared marks the Redis implementation as the coordination face.
func (s *redisStore) Shared() bool { return true }

// Available reports whether the last store operation succeeded.
func (s *redisStore) Available() bool {
	return atomic.LoadInt64(&s.downSince) == 0
}

// DownFor is the elapsed outage; zero while available.
func (s *redisStore) DownFor() time.Duration {
	start := atomic.LoadInt64(&s.downSince)
	if start == 0 {
		return 0
	}
	return s.now().Sub(time.Unix(0, start))
}

// mark records the outcome of one store operation for the outage face
// (§10 共享状态不可用). The first failure starts the outage clock and emits
// the unavailable event; the first success after an outage clears it and
// emits the recovered event with the outage duration (§9.1).
func (s *redisStore) mark(err error) {
	if err == nil {
		if prev := atomic.SwapInt64(&s.downSince, 0); prev != 0 && s.event != nil {
			s.event("recovered", s.now().Sub(time.Unix(0, prev)), nil)
		}
		return
	}
	if atomic.CompareAndSwapInt64(&s.downSince, 0, s.now().UnixNano()) && s.event != nil {
		s.event("unavailable", 0, err)
	}
}

// Stage-two Lua scripts. Every multi-step write is one script so a concurrent
// instance can never observe or create a half-applied transition; the CAS
// scripts compare the exact raw value read by the Go caller, which is the
// optimistic concurrency the cross-instance single-write needs.
var (
	// casSet: KEYS[1]=row, ARGV[1]=expected raw, ARGV[2]=new raw, ARGV[3]=ttl ms.
	// Returns 1 applied, 0 changed since read, -1 row missing.
	scriptCASSet = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if not cur then return -1 end
if cur ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
return 1`)

	// casStart: KEYS[1]=row, KEYS[2]=queue marker; same ARGV as casSet.
	scriptCASStart = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if not cur then return -1 end
if cur ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
redis.call('DEL', KEYS[2])
return 1`)

	// casTerminal: KEYS[1]=row, KEYS[2]=settled, KEYS[3]=marker;
	// ARGV[1]=expected, ARGV[2]=new, ARGV[3]=settled, ARGV[4]=ttl ms.
	scriptCASTerminal = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if not cur then return -1 end
if cur ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[4])
redis.call('SET', KEYS[2], ARGV[3], 'PX', ARGV[4])
redis.call('DEL', KEYS[3])
return 1`)

	// setIndexed: KEYS[1]=row, KEYS[2]=job index; ARGV[1]=row raw,
	// ARGV[2]=index value, ARGV[3]=ttl ms.
	scriptSetIndexed = redis.NewScript(`
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[3])
redis.call('SET', KEYS[2], ARGV[2])
return 1`)

	// enqueue: KEYS[1]=marker, KEYS[2]=row; ARGV[1]=row raw, ARGV[2]=marker raw,
	// ARGV[3]=ttl ms. Returns 1 when written, 0 when the queue row already
	// exists with its mapping row (the idempotent hit).
	scriptEnqueue = redis.NewScript(`
local marker = redis.call('EXISTS', KEYS[1])
local row = redis.call('EXISTS', KEYS[2])
if marker == 1 and row == 1 then return 0 end
redis.call('SET', KEYS[2], ARGV[1], 'PX', ARGV[3])
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
return 1`)

	// deleteFull: KEYS[1]=row, KEYS[2]=marker, KEYS[3]=job index.
	scriptDeleteFull = redis.NewScript(`
redis.call('DEL', KEYS[1])
redis.call('DEL', KEYS[2])
redis.call('DEL', KEYS[3])
return 1`)

	// slotReserve: KEYS[1]=counter, KEYS[2]=reservation zset;
	// ARGV[1]=limit, ARGV[2]=now ms, ARGV[3]=reservation ttl ms,
	// ARGV[4]=reservation id. Expired reservations are purged (and their
	// slots returned) before the cap check, so a dead reserver self-heals on
	// the next claim. The counter is the cap check (AC-76).
	scriptSlotReserve = redis.NewScript(`
local now = tonumber(ARGV[2])
local before = redis.call('ZCARD', KEYS[2])
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
local purged = before - redis.call('ZCARD', KEYS[2])
local n = tonumber(redis.call('GET', KEYS[1]) or '0') - purged
if n < 0 then n = 0 end
if n >= tonumber(ARGV[1]) then
  redis.call('SET', KEYS[1], n)
  return 0
end
redis.call('SET', KEYS[1], n + 1)
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[3]), ARGV[4])
return 1`)

	// slotConsume: pop one reservation, counter unchanged (the row counts).
	scriptSlotConsume = redis.NewScript(`
if redis.call('ZCARD', KEYS[2]) > 0 then redis.call('ZPOPMIN', KEYS[2]) end
return 1`)

	// slotRelease (reservation): return the slot and drop one reservation.
	scriptSlotRelease = redis.NewScript(`
local n = tonumber(redis.call('GET', KEYS[1]) or '0')
if n > 0 then n = n - 1 end
redis.call('SET', KEYS[1], n)
if redis.call('ZCARD', KEYS[2]) > 0 then redis.call('ZPOPMIN', KEYS[2]) end
return 1`)

	// slotReleaseEntry: return an entry's slot, leaving reservations alone.
	scriptSlotReleaseEntry = redis.NewScript(`
local n = tonumber(redis.call('GET', KEYS[1]) or '0')
if n > 0 then n = n - 1 end
redis.call('SET', KEYS[1], n)
return 1`)

	// slotSnapshot: purge expired reservations, adjust the counter, and
	// return {counter, live reservation count} from one atomic read.
	scriptSlotSnapshot = redis.NewScript(`
local now = tonumber(ARGV[1])
local before = redis.call('ZCARD', KEYS[2])
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
local purged = before - redis.call('ZCARD', KEYS[2])
local n = tonumber(redis.call('GET', KEYS[1]) or '0') - purged
if n < 0 then n = 0 end
redis.call('SET', KEYS[1], n)
return {n, redis.call('ZCARD', KEYS[2])}`)

	// slotCAS: write next only when the counter still equals expected.
	scriptSlotCAS = redis.NewScript(`
local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
if cur ~= tonumber(ARGV[1]) then return 0 end
redis.call('SET', KEYS[1], ARGV[2])
return 1`)

	// lockAcquire: take the lock when it is free or its holder's lease has
	// expired. The value carries the owner and the deadline
	// ("owner\x00untilMillis"), so a store whose TTL collection is lazy never
	// keeps a dead holder's lock forever (and tests drive expiry by time, not
	// by a store clock). ARGV: new value, now ms, ttl ms.
	scriptLockAcquire = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
local now = tonumber(ARGV[2])
if cur then
  local sep = string.find(cur, '\0', 1, true)
  local untilMs = nil
  if sep then untilMs = tonumber(string.sub(cur, sep + 1)) end
  if not untilMs or untilMs > now then return 0 end
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[3])
return 1`)

	// lockRelease: delete only when the owner still holds the lock.
	scriptLockRelease = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if not cur then return 0 end
local sep = string.find(cur, '\0', 1, true)
local owner = string.sub(cur, 1, (sep or 0) - 1)
if owner == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0`)

	// leaderAcquire: take the lease when it is free or expired (same value
	// shape as the task lock). ARGV: new value, now ms, ttl ms.
	scriptLeaderAcquire = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
local now = tonumber(ARGV[2])
if cur then
  local sep = string.find(cur, '\0', 1, true)
  local untilMs = nil
  if sep then untilMs = tonumber(string.sub(cur, sep + 1)) end
  if not untilMs or untilMs > now then return 0 end
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[3])
return 1`)

	// leaderRenew: extend only when the owner still holds the lease.
	// ARGV: owner, new value, ttl ms, now ms.
	scriptLeaderRenew = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if not cur then return 0 end
local sep = string.find(cur, '\0', 1, true)
local owner = string.sub(cur, 1, (sep or 0) - 1)
if owner ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
return 1`)
)

// svcInstanceID is the lease/lock value: the owner plus the deadline the next
// acquisition compares against.
func svcInstanceID(owner string, until time.Time) string {
	return owner + "\x00" + strconv.FormatInt(until.UnixMilli(), 10)
}

// svcOwner strips the deadline from a lease/lock value.
func svcOwner(raw string) string {
	if idx := strings.LastIndexByte(raw, 0); idx >= 0 {
		return raw[:idx]
	}
	return raw
}

// errSharedConflict reports an optimistic CAS that kept losing for a full
// retry budget; the caller's transition is not applied and the next attempt
// reads the winning row.
var errSharedConflict = errors.New("shared state: concurrent update conflict")

// casRetries bounds the optimistic read-modify-write loop. Conflicts only come
// from a concurrent instance mutating the same row, which the per-task write
// lock already serializes for the multi-step paths; the retry is the safety
// net for the CAS-only paths (registration, start, terminal).
const casRetries = 8

// entryTTL is the retention applied to every stage-two row write.
func (s *redisStore) entryTTL() time.Duration {
	if s.cfg.EntryTTL > 0 {
		return s.cfg.EntryTTL
	}
	return DefaultTTL
}

func (s *redisStore) queueKey(ws, taskID string) string {
	return keyQueue + ws + ":" + taskID
}

func (s *redisStore) lockKey(ws, taskID string) string {
	return keyLock + ws + ":" + taskID
}

func (s *redisStore) jobIndexKey(jobName string) string {
	return keyJobIndex + jobName
}

// casUpdate is the optimistic read-modify-write: mutate runs against the row
// read from the store, and the write applies only when the row is still
// byte-identical. write=false (or an error) leaves the row untouched.
func (s *redisStore) casUpdate(ctx context.Context, key string, mutate func(registry.TaskEntry) (registry.TaskEntry, bool, error)) (registry.TaskEntry, bool, error) {
	for range casRetries {
		raw, err := s.rdb.Get(ctx, key).Bytes()
		if errors.Is(err, redis.Nil) {
			s.mark(nil)
			return registry.TaskEntry{}, false, nil
		}
		if err != nil {
			s.mark(err)
			return registry.TaskEntry{}, false, err
		}
		cur, err := s.unmarshal(raw)
		if err != nil {
			s.mark(err)
			return registry.TaskEntry{}, false, err
		}
		next, write, err := mutate(cur)
		if err != nil || !write {
			s.mark(err)
			return next, false, err
		}
		nextRaw, err := s.marshal(next)
		if err != nil {
			return registry.TaskEntry{}, false, err
		}
		code, err := scriptCASSet.Run(ctx, s.rdb, []string{key}, raw, nextRaw, s.entryTTL().Milliseconds()).Int()
		if err != nil {
			s.mark(err)
			return registry.TaskEntry{}, false, err
		}
		switch code {
		case 1:
			s.mark(nil)
			return next, true, nil
		case -1:
			return registry.TaskEntry{}, false, nil
		}
	}
	return registry.TaskEntry{}, false, errSharedConflict
}

// SetEntry writes the full mapping row and its Job index.
func (s *redisStore) SetEntry(ctx context.Context, ws, taskID string, e registry.TaskEntry) error {
	raw, err := s.marshal(e)
	if err != nil {
		return err
	}
	// A row whose Job name moved leaves the old index behind; drop it so a
	// name lookup never resolves to a row that no longer owns it.
	if old, ok, err := s.GetEntry(ctx, ws, taskID); err == nil && ok && old.JobName != "" && old.JobName != e.JobName {
		_ = s.rdb.Del(ctx, s.jobIndexKey(old.JobName)).Err()
	}
	index := ws + "\x00" + taskID
	err = scriptSetIndexed.Run(ctx, s.rdb,
		[]string{s.entryKey(ws, taskID), s.jobIndexKey(e.JobName)},
		raw, index, s.entryTTL().Milliseconds()).Err()
	s.mark(err)
	return err
}

// MergeEntry applies the in-place patch through the optimistic CAS loop: a
// concurrent gate write wins and the merge re-reads, so a late field update
// can never revert a transition.
func (s *redisStore) MergeEntry(ctx context.Context, ws, taskID string, p registry.EntryPatch) error {
	_, _, err := s.casUpdate(ctx, s.entryKey(ws, taskID), func(e registry.TaskEntry) (registry.TaskEntry, bool, error) {
		changed := false
		if p.LastStatusSeen != nil && e.LastStatusSeen != *p.LastStatusSeen {
			e.LastStatusSeen = *p.LastStatusSeen
			changed = true
		}
		if p.LeaseKeepUntil != nil && !e.LeaseKeepUntil.Equal(*p.LeaseKeepUntil) {
			e.LeaseKeepUntil = *p.LeaseKeepUntil
			changed = true
		}
		if p.NodeName != nil && e.NodeName != *p.NodeName {
			e.NodeName = *p.NodeName
			changed = true
		}
		if p.Result != nil && string(e.Result) != *p.Result {
			e.Result = registry.Result(*p.Result)
			changed = true
		}
		if p.FailureReason != nil && e.FailureReason != *p.FailureReason {
			e.FailureReason = *p.FailureReason
			changed = true
		}
		if p.TokenHash != nil && e.TokenHash != *p.TokenHash {
			e.TokenHash = *p.TokenHash
			changed = true
		}
		return e, changed, nil
	})
	return err
}

// queueMarker is the queue row's small body: the marker exists to make the
// single-enqueue idempotent and the queue depth countable; the payload and the
// delivery accounting live in the mapping row (同源, §5).
type queueMarker struct {
	TaskID      string    `json:"task_id"`
	WorkspaceID string    `json:"workspace_id"`
	EnqueuedAt  time.Time `json:"enqueued_at"`
}

// EnqueueEntry performs the single enqueue (§5 单次入队).
func (s *redisStore) EnqueueEntry(ctx context.Context, ws, taskID string, e registry.TaskEntry) (bool, error) {
	raw, err := s.marshal(e)
	if err != nil {
		return false, err
	}
	marker, err := json.Marshal(queueMarker{TaskID: taskID, WorkspaceID: ws, EnqueuedAt: s.now()})
	if err != nil {
		return false, err
	}
	n, err := scriptEnqueue.Run(ctx, s.rdb,
		[]string{s.queueKey(ws, taskID), s.entryKey(ws, taskID)},
		raw, marker, s.entryTTL().Milliseconds()).Int()
	s.mark(err)
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// PickupDelivery is the atomic S-6 take (§5 单次派发). The delivery body and
// the accounting are read and written in one CAS step, so two instances
// claiming the same Job can never both win.
func (s *redisStore) PickupDelivery(ctx context.Context, ws, taskID string, window time.Duration) (Pickup, error) {
	key := s.entryKey(ws, taskID)
	now := s.now()
	var out Pickup
	entry, _, err := s.casUpdate(ctx, key, func(e registry.TaskEntry) (registry.TaskEntry, bool, error) {
		out = Pickup{Entry: e}
		if e.State == registry.StateTerminal {
			return e, false, nil
		}
		if !e.StartedAt.IsZero() {
			return e, false, nil
		}
		if len(e.Payload) == 0 {
			// A rebuilt row carries no body; the queue cannot deliver it and
			// the convergence path owns the row (§4.3).
			return e, false, nil
		}
		if !e.DeliveredAt.IsZero() {
			if !e.DeliveryDeadline.IsZero() && e.DeliveryDeadline.After(now) {
				// Inside the confirmation window: the empty batch that
				// answers a lost response, counted on the entry (§4.1).
				e.ClaimsAfterDelivery++
				out.Held = true
				return e, true, nil
			}
			if e.DeliveryDeadline.IsZero() {
				// The re-delivery exit is switched off (§5 取值前提).
				return e, false, nil
			}
			out.PrevDeliveredAt = e.DeliveredAt
			out.PrevClaims = e.ClaimsAfterDelivery
		} else if e.State != registry.StateDaemonRegistered {
			return e, false, nil
		}
		e.State = registry.StateDelivering
		e.DeliveredAt = now
		e.Deliveries++
		e.ClaimsAfterDelivery = 0
		if window > 0 {
			e.DeliveryDeadline = now.Add(window)
		}
		out.Delivered = true
		out.Entry = e
		return e, true, nil
	})
	if err != nil {
		return Pickup{}, err
	}
	if entry.TaskID == "" {
		out.Missing = true
	}
	if !out.Delivered {
		return out, nil
	}
	out.Entry = entry
	return out, nil
}

// ConfirmStart writes the once-only start fact and drops the queue marker.
func (s *redisStore) ConfirmStart(ctx context.Context, ws, taskID string, at time.Time, advance bool) (registry.TaskEntry, bool, error) {
	key := s.entryKey(ws, taskID)
	var first bool
	for range casRetries {
		raw, err := s.rdb.Get(ctx, key).Bytes()
		if errors.Is(err, redis.Nil) {
			s.mark(nil)
			return registry.TaskEntry{}, false, nil
		}
		if err != nil {
			s.mark(err)
			return registry.TaskEntry{}, false, err
		}
		cur, err := s.unmarshal(raw)
		if err != nil {
			s.mark(err)
			return registry.TaskEntry{}, false, err
		}
		if cur.State == registry.StateTerminal {
			return cur, false, nil
		}
		first = cur.StartedAt.IsZero()
		changed := false
		if first {
			cur.StartedAt = at
			changed = true
		}
		if advance && cur.State != registry.StateRunning {
			cur.State = registry.StateRunning
			changed = true
		}
		if !changed {
			return cur, false, nil
		}
		nextRaw, err := s.marshal(cur)
		if err != nil {
			return registry.TaskEntry{}, false, err
		}
		code, err := scriptCASStart.Run(ctx, s.rdb,
			[]string{key, s.queueKey(ws, taskID)},
			raw, nextRaw, s.entryTTL().Milliseconds()).Int()
		if err != nil {
			s.mark(err)
			return registry.TaskEntry{}, false, err
		}
		switch code {
		case 1:
			s.mark(nil)
			return cur, first, nil
		case -1:
			return registry.TaskEntry{}, false, nil
		}
	}
	return registry.TaskEntry{}, false, errSharedConflict
}

// CommitDelivering records one delivery commit under the S6 gate (§5).
func (s *redisStore) CommitDelivering(ctx context.Context, ws, taskID string, at time.Time, payload []byte, window time.Duration) (registry.TaskEntry, bool, error) {
	entry, ok, err := s.casUpdate(ctx, s.entryKey(ws, taskID), func(e registry.TaskEntry) (registry.TaskEntry, bool, error) {
		if !registry.Deliverable(e, at) {
			return e, false, nil
		}
		e.State = registry.StateDelivering
		e.DeliveredAt = at
		e.Deliveries++
		e.ClaimsAfterDelivery = 0
		e.Payload = append([]byte(nil), payload...)
		if window > 0 {
			e.DeliveryDeadline = at.Add(window)
		}
		return e, true, nil
	})
	return entry, ok, err
}

// CommitJobCreated records the apiserver creation timestamp.
func (s *redisStore) CommitJobCreated(ctx context.Context, ws, taskID string, at time.Time) (registry.TaskEntry, bool, error) {
	entry, ok, err := s.casUpdate(ctx, s.entryKey(ws, taskID), func(e registry.TaskEntry) (registry.TaskEntry, bool, error) {
		if e.State == registry.StateTerminal {
			return e, false, nil
		}
		if e.State == registry.StatePending {
			e.State = registry.StateJobCreated
		}
		e.JobCreatedAt = at
		return e, true, nil
	})
	return entry, ok, err
}

// CommitDaemonRegistered records the S-4 registration.
func (s *redisStore) CommitDaemonRegistered(ctx context.Context, ws, taskID string, at time.Time) (registry.TaskEntry, bool, error) {
	entry, ok, err := s.casUpdate(ctx, s.entryKey(ws, taskID), func(e registry.TaskEntry) (registry.TaskEntry, bool, error) {
		if e.State == registry.StateTerminal || e.State == registry.StatePending {
			return e, false, nil
		}
		if e.State == registry.StateJobCreated || e.State == registry.StateDaemonRegistered {
			e.State = registry.StateDaemonRegistered
		}
		e.DaemonRegisteredAt = at
		return e, true, nil
	})
	return entry, ok, err
}

// CommitTerminal writes the terminal verdict, the settled record and drops the
// queue marker in one atomic step (§4.2, §5).
func (s *redisStore) CommitTerminal(ctx context.Context, ws, taskID string, result registry.Result, reason string, completedAt, drainDeadline time.Time) (registry.TaskEntry, bool, error) {
	key := s.entryKey(ws, taskID)
	for range casRetries {
		raw, err := s.rdb.Get(ctx, key).Bytes()
		if errors.Is(err, redis.Nil) {
			s.mark(nil)
			return registry.TaskEntry{}, false, nil
		}
		if err != nil {
			s.mark(err)
			return registry.TaskEntry{}, false, err
		}
		cur, err := s.unmarshal(raw)
		if err != nil {
			s.mark(err)
			return registry.TaskEntry{}, false, err
		}
		if cur.State == registry.StateTerminal {
			return cur, false, nil
		}
		cur.State = registry.StateTerminal
		cur.JobPhase = registry.JobPhaseDraining
		cur.DrainDeadline = drainDeadline
		cur.CompletedAt = completedAt
		cur.Result = result
		if reason != "" {
			cur.FailureReason = reason
		}
		nextRaw, err := s.marshal(cur)
		if err != nil {
			return registry.TaskEntry{}, false, err
		}
		settled := cur
		settled.Payload = nil
		settled.TokenHash = ""
		settled.DecidedReport = nil
		settledRaw, err := s.marshal(settled)
		if err != nil {
			return registry.TaskEntry{}, false, err
		}
		code, err := scriptCASTerminal.Run(ctx, s.rdb,
			[]string{key, s.doneKey(ws, taskID), s.queueKey(ws, taskID)},
			raw, nextRaw, settledRaw, s.entryTTL().Milliseconds()).Int()
		if err != nil {
			s.mark(err)
			return registry.TaskEntry{}, false, err
		}
		switch code {
		case 1:
			s.mark(nil)
			return cur, true, nil
		case -1:
			return registry.TaskEntry{}, false, nil
		}
	}
	return registry.TaskEntry{}, false, errSharedConflict
}

// CommitDecidedReport records the decided terminal body once (§4.4 定论体).
func (s *redisStore) CommitDecidedReport(ctx context.Context, ws, taskID string, body []byte) (registry.TaskEntry, bool, error) {
	entry, ok, err := s.casUpdate(ctx, s.entryKey(ws, taskID), func(e registry.TaskEntry) (registry.TaskEntry, bool, error) {
		if len(e.DecidedReport) != 0 || len(body) == 0 {
			return e, false, nil
		}
		e.DecidedReport = append([]byte(nil), body...)
		return e, true, nil
	})
	return entry, ok, err
}

// SettleNoResult clears the verdict of a terminal row whose report the server
// answered with "task gone" (§4.4).
func (s *redisStore) SettleNoResult(ctx context.Context, ws, taskID string, expected registry.Result) (registry.TaskEntry, bool, error) {
	key := s.entryKey(ws, taskID)
	for range casRetries {
		raw, err := s.rdb.Get(ctx, key).Bytes()
		if errors.Is(err, redis.Nil) {
			s.mark(nil)
			return registry.TaskEntry{}, false, nil
		}
		if err != nil {
			s.mark(err)
			return registry.TaskEntry{}, false, err
		}
		cur, err := s.unmarshal(raw)
		if err != nil {
			s.mark(err)
			return registry.TaskEntry{}, false, err
		}
		if cur.State != registry.StateTerminal || cur.Result != expected {
			return cur, false, nil
		}
		cur.Result = ""
		cur.FailureReason = ""
		nextRaw, err := s.marshal(cur)
		if err != nil {
			return registry.TaskEntry{}, false, err
		}
		settled := cur
		settled.Payload = nil
		settled.TokenHash = ""
		settled.DecidedReport = nil
		settledRaw, err := s.marshal(settled)
		if err != nil {
			return registry.TaskEntry{}, false, err
		}
		code, err := scriptCASTerminal.Run(ctx, s.rdb,
			[]string{key, s.doneKey(ws, taskID), s.queueKey(ws, taskID)},
			raw, nextRaw, settledRaw, s.entryTTL().Milliseconds()).Int()
		if err != nil {
			s.mark(err)
			return registry.TaskEntry{}, false, err
		}
		switch code {
		case 1:
			s.mark(nil)
			return cur, true, nil
		case -1:
			return registry.TaskEntry{}, false, nil
		}
	}
	return registry.TaskEntry{}, false, errSharedConflict
}

// DeleteEntry removes the live row, its queue marker and its Job index.
func (s *redisStore) DeleteEntry(ctx context.Context, ws, taskID string) error {
	jobName := ""
	if e, ok, err := s.GetEntry(ctx, ws, taskID); err == nil && ok {
		jobName = e.JobName
	}
	keys := []string{s.entryKey(ws, taskID), s.queueKey(ws, taskID), s.jobIndexKey(jobName)}
	err := scriptDeleteFull.Run(ctx, s.rdb, keys).Err()
	s.mark(err)
	return err
}

// DropQueue removes the queue marker only.
func (s *redisStore) DropQueue(ctx context.Context, ws, taskID string) error {
	err := s.rdb.Del(ctx, s.queueKey(ws, taskID)).Err()
	s.mark(err)
	return err
}

// QueueDepth counts the live queue markers.
func (s *redisStore) QueueDepth(ctx context.Context) (int64, error) {
	var n int64
	iter := s.rdb.Scan(ctx, 0, keyQueue+"*", 256).Iterator()
	for iter.Next(ctx) {
		n++
	}
	err := iter.Err()
	s.mark(err)
	return n, err
}

// GetEntryByJob resolves a Job name through the shared index.
func (s *redisStore) GetEntryByJob(ctx context.Context, jobName string) (registry.TaskEntry, bool, error) {
	raw, err := s.rdb.Get(ctx, s.jobIndexKey(jobName)).Bytes()
	if errors.Is(err, redis.Nil) {
		s.mark(nil)
		return registry.TaskEntry{}, false, nil
	}
	if err != nil {
		s.mark(err)
		return registry.TaskEntry{}, false, err
	}
	idx := strings.LastIndexByte(string(raw), 0)
	if idx <= 0 || idx == len(raw)-1 {
		return registry.TaskEntry{}, false, nil
	}
	ws, taskID := string(raw[:idx]), string(raw[idx+1:])
	return s.GetEntry(ctx, ws, taskID)
}

// LockTask acquires the per-task write lock; a holder whose lease deadline has
// passed is replaced.
func (s *redisStore) LockTask(ctx context.Context, ws, taskID, owner string, ttl time.Duration) (bool, error) {
	now := s.now()
	n, err := scriptLockAcquire.Run(ctx, s.rdb, []string{s.lockKey(ws, taskID)},
		svcInstanceID(owner, now.Add(ttl)), now.UnixMilli(), ttl.Milliseconds()).Int()
	s.mark(err)
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// UnlockTask releases the lock only when owner still holds it.
func (s *redisStore) UnlockTask(ctx context.Context, ws, taskID, owner string) error {
	err := scriptLockRelease.Run(ctx, s.rdb, []string{s.lockKey(ws, taskID)}, owner).Err()
	s.mark(err)
	return err
}

// slotKeys names the counter and reservation set of one level.
func slotKeys(ws string) (counter, reservations string) {
	if ws == "" {
		return keySlotAll, keySlotResAll
	}
	return keySlotWS + ws, keySlotResWS + ws
}

// reservationTTL bounds how long a reservation can outlive its reserver: it
// must cover one claim batch (C3 + OnClaim up to the Job-name wait).
const reservationTTL = 5 * time.Minute

// ReserveSlot reserves one in-flight slot below limit (AC-76).
func (s *redisStore) ReserveSlot(ctx context.Context, ws string, limit int) (bool, error) {
	counter, reservations := slotKeys(ws)
	now := s.now()
	id := strconv.FormatInt(now.UnixNano(), 36) + "-" + strconv.FormatInt(s.resSeq.Add(1), 36)
	n, err := scriptSlotReserve.Run(ctx, s.rdb, []string{counter, reservations},
		limit, now.UnixMilli(), reservationTTL.Milliseconds(), id).Int()
	s.mark(err)
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// ConsumeSlotReservation converts one reservation into entry ownership.
func (s *redisStore) ConsumeSlotReservation(ctx context.Context, ws string) error {
	_, reservations := slotKeys(ws)
	err := scriptSlotConsume.Run(ctx, s.rdb, []string{reservations}).Err()
	s.mark(err)
	return err
}

// ReleaseSlot abandons one reservation and returns its slot.
func (s *redisStore) ReleaseSlot(ctx context.Context, ws string) error {
	counter, reservations := slotKeys(ws)
	err := scriptSlotRelease.Run(ctx, s.rdb, []string{counter, reservations}).Err()
	s.mark(err)
	return err
}

// ReleaseEntry returns an entry's in-flight slot.
func (s *redisStore) ReleaseEntry(ctx context.Context, ws string) error {
	counter, _ := slotKeys(ws)
	err := scriptSlotReleaseEntry.Run(ctx, s.rdb, []string{counter}).Err()
	s.mark(err)
	return err
}

// SlotSnapshot atomically purges expired reservations and returns the
// counter and live reservation count.
func (s *redisStore) SlotSnapshot(ctx context.Context, ws string) (int64, int64, error) {
	counter, reservations := slotKeys(ws)
	values, err := scriptSlotSnapshot.Run(ctx, s.rdb, []string{counter, reservations}, s.now().UnixMilli()).Int64Slice()
	s.mark(err)
	if err != nil {
		return 0, 0, err
	}
	if len(values) != 2 {
		return 0, 0, fmt.Errorf("shared state: slot snapshot returned %d values", len(values))
	}
	return values[0], values[1], nil
}

// CompareAndSetSlots writes next only when the counter still equals expected.
func (s *redisStore) CompareAndSetSlots(ctx context.Context, ws string, expected, next int64) (bool, error) {
	counter, _ := slotKeys(ws)
	n, err := scriptSlotCAS.Run(ctx, s.rdb, []string{counter}, expected, next).Int()
	s.mark(err)
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// ClearSlotReservations drops every pending reservation of one level.
func (s *redisStore) ClearSlotReservations(ctx context.Context, ws string) error {
	_, reservations := slotKeys(ws)
	err := s.rdb.Del(ctx, reservations).Err()
	s.mark(err)
	return err
}

// SetSlots writes the reconciled counter value.
func (s *redisStore) SetSlots(ctx context.Context, ws string, n int64) error {
	key := keySlotAll
	if ws != "" {
		key = keySlotWS + ws
	}
	err := s.rdb.Set(ctx, key, n, 0).Err()
	s.mark(err)
	return err
}

// Slots reads the counter.
func (s *redisStore) Slots(ctx context.Context, ws string) (int64, error) {
	key := keySlotAll
	if ws != "" {
		key = keySlotWS + ws
	}
	raw, err := s.rdb.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		s.mark(nil)
		return 0, nil
	}
	if err != nil {
		s.mark(err)
		return 0, err
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("shared state: corrupt slot counter %s: %w", key, err)
	}
	s.mark(nil)
	return n, nil
}

// AcquireLeadership takes the control-plane lease when it is free (§10 控制面
// 接管有界).
func (s *redisStore) AcquireLeadership(ctx context.Context, owner string, ttl time.Duration) (bool, error) {
	now := s.now()
	n, err := scriptLeaderAcquire.Run(ctx, s.rdb, []string{keyLeader},
		svcInstanceID(owner, now.Add(ttl)), now.UnixMilli(), ttl.Milliseconds()).Int()
	s.mark(err)
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// RenewLeadership extends the lease when owner still holds it.
func (s *redisStore) RenewLeadership(ctx context.Context, owner string, ttl time.Duration) (bool, error) {
	now := s.now()
	n, err := scriptLeaderRenew.Run(ctx, s.rdb, []string{keyLeader},
		owner, svcInstanceID(owner, now.Add(ttl)), ttl.Milliseconds()).Int()
	s.mark(err)
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// ReleaseLeadership drops the lease when owner still holds it.
func (s *redisStore) ReleaseLeadership(ctx context.Context, owner string) error {
	err := scriptLockRelease.Run(ctx, s.rdb, []string{keyLeader}, owner).Err()
	s.mark(err)
	return err
}

// LeadershipHolder names the current lease holder ("" when free).
func (s *redisStore) LeadershipHolder(ctx context.Context) (string, error) {
	raw, err := s.rdb.Get(ctx, keyLeader).Result()
	if errors.Is(err, redis.Nil) {
		s.mark(nil)
		return "", nil
	}
	if err != nil {
		s.mark(err)
		return "", err
	}
	return svcOwner(raw), nil
}

// HeartbeatInstance refreshes one instance's liveness row.
func (s *redisStore) HeartbeatInstance(ctx context.Context, id string, ttl time.Duration) error {
	err := s.rdb.Set(ctx, keyInstance+id, s.now().Unix(), ttl).Err()
	s.mark(err)
	return err
}

// ListInstances lists the live instance ids.
func (s *redisStore) ListInstances(ctx context.Context) ([]string, error) {
	var out []string
	iter := s.rdb.Scan(ctx, 0, keyInstance+"*", 256).Iterator()
	for iter.Next(ctx) {
		out = append(out, strings.TrimPrefix(iter.Val(), keyInstance))
	}
	err := iter.Err()
	s.mark(err)
	return out, err
}

// PutDeclaration stores the registration identity of one workspace.
func (s *redisStore) PutDeclaration(ctx context.Context, ws string, raw []byte) error {
	err := s.rdb.Set(ctx, keyDecl+ws, raw, 0).Err()
	s.mark(err)
	return err
}

// GetDeclaration reads the registration identity of one workspace.
func (s *redisStore) GetDeclaration(ctx context.Context, ws string) ([]byte, bool, error) {
	raw, err := s.rdb.Get(ctx, keyDecl+ws).Bytes()
	if errors.Is(err, redis.Nil) {
		s.mark(nil)
		return nil, false, nil
	}
	if err != nil {
		s.mark(err)
		return nil, false, err
	}
	return raw, true, nil
}

// PutCatalogSlot stores one real runtime's catalog slot.
func (s *redisStore) PutCatalogSlot(ctx context.Context, runtimeID string, raw []byte, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = s.entryTTL()
	}
	err := s.rdb.Set(ctx, keyCatalog+runtimeID, raw, ttl).Err()
	s.mark(err)
	return err
}

// GetCatalogSlot reads one catalog slot.
func (s *redisStore) GetCatalogSlot(ctx context.Context, runtimeID string) ([]byte, bool, error) {
	raw, err := s.rdb.Get(ctx, keyCatalog+runtimeID).Bytes()
	if errors.Is(err, redis.Nil) {
		s.mark(nil)
		return nil, false, nil
	}
	if err != nil {
		s.mark(err)
		return nil, false, err
	}
	return raw, true, nil
}

// DeleteCatalogSlot removes one catalog slot.
func (s *redisStore) DeleteCatalogSlot(ctx context.Context, runtimeID string) error {
	err := s.rdb.Del(ctx, keyCatalog+runtimeID).Err()
	s.mark(err)
	return err
}

// PutSessionNode stores one soft-affinity row (§4.5 共享索引).
func (s *redisStore) PutSessionNode(ctx context.Context, ws, key string, raw []byte, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = s.entryTTL()
	}
	err := s.rdb.Set(ctx, keySession+ws+":"+key, raw, ttl).Err()
	s.mark(err)
	return err
}

// GetSessionNode reads one soft-affinity row.
func (s *redisStore) GetSessionNode(ctx context.Context, ws, key string) ([]byte, bool, error) {
	raw, err := s.rdb.Get(ctx, keySession+ws+":"+key).Bytes()
	if errors.Is(err, redis.Nil) {
		s.mark(nil)
		return nil, false, nil
	}
	if err != nil {
		s.mark(err)
		return nil, false, err
	}
	return raw, true, nil
}

// DeleteSessionNode removes one soft-affinity row.
func (s *redisStore) DeleteSessionNode(ctx context.Context, ws, key string) error {
	err := s.rdb.Del(ctx, keySession+ws+":"+key).Err()
	s.mark(err)
	return err
}

// ListSessionNodes lists every soft-affinity row.
func (s *redisStore) ListSessionNodes(ctx context.Context) (map[string][]byte, error) {
	out := make(map[string][]byte)
	iter := s.rdb.Scan(ctx, 0, keySession+"*", 256).Iterator()
	for iter.Next(ctx) {
		raw, err := s.rdb.Get(ctx, iter.Val()).Bytes()
		if err != nil {
			continue
		}
		out[strings.TrimPrefix(iter.Val(), keySession)] = raw
	}
	err := iter.Err()
	s.mark(err)
	return out, err
}

// PutProbeSession stores one probe session row.
func (s *redisStore) PutProbeSession(ctx context.Context, jobName string, raw []byte, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = s.entryTTL()
	}
	err := s.rdb.Set(ctx, keyProbe+jobName, raw, ttl).Err()
	s.mark(err)
	return err
}

// GetProbeSession reads one probe session row.
func (s *redisStore) GetProbeSession(ctx context.Context, jobName string) ([]byte, bool, error) {
	raw, err := s.rdb.Get(ctx, keyProbe+jobName).Bytes()
	if errors.Is(err, redis.Nil) {
		s.mark(nil)
		return nil, false, nil
	}
	if err != nil {
		s.mark(err)
		return nil, false, err
	}
	return raw, true, nil
}

// CASProbeSession replaces one probe session row only when unchanged.
func (s *redisStore) CASProbeSession(ctx context.Context, jobName string, expected, next []byte, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		ttl = s.entryTTL()
	}
	code, err := scriptCASSet.Run(ctx, s.rdb, []string{keyProbe + jobName}, expected, next, ttl.Milliseconds()).Int()
	s.mark(err)
	if err != nil {
		return false, err
	}
	return code == 1, nil
}

// DeleteProbeSession removes one probe session row.
func (s *redisStore) DeleteProbeSession(ctx context.Context, jobName string) error {
	err := s.rdb.Del(ctx, keyProbe+jobName).Err()
	s.mark(err)
	return err
}

// ListProbeSessions lists every probe session row.
func (s *redisStore) ListProbeSessions(ctx context.Context) (map[string][]byte, error) {
	out := make(map[string][]byte)
	iter := s.rdb.Scan(ctx, 0, keyProbe+"*", 256).Iterator()
	for iter.Next(ctx) {
		raw, err := s.rdb.Get(ctx, iter.Val()).Bytes()
		if err != nil {
			continue
		}
		out[strings.TrimPrefix(iter.Val(), keyProbe)] = raw
	}
	err := iter.Err()
	s.mark(err)
	return out, err
}
