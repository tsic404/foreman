package state

// The boundary between a caller's willingness to wait and the store's own
// budget (02-contracts.md §10 共享状态不可用的判定; ADR-054 决策 1–4): every
// operation runs detached from the caller's deadline and cancellation, so
// neither can shorten it nor be read as evidence. The caller's context decides
// only how long that caller waits; when it ends first the caller answers its
// own error and the operation still runs to the store's bound, which is what
// keeps a stalled store detectable by callers on a shorter budget.

import (
	"context"
	"sync"
	"time"

	"github.com/tsic404/foreman/internal/registry"
)

// opBoundKey marks a context that already runs under the store's budget, so a
// store operation calling another one reuses that budget instead of stacking a
// second full one.
type opBoundKey struct{}

// opTracker counts the operations still running after the caller that started
// them stopped waiting. Such an operation keeps forming its conclusion (§10
// 判定: a caller's exit does not void it), so a test that asserts on that
// conclusion waits here for the count to fall to zero instead of racing the
// goroutine that records it.
type opTracker struct {
	mu     sync.Mutex
	cond   *sync.Cond
	active int
}

// begin registers one operation that outlives its caller.
func (o *opTracker) begin() {
	o.mu.Lock()
	o.active++
	o.mu.Unlock()
}

// end retires it and wakes any waiter that reaches zero.
func (o *opTracker) end() {
	o.mu.Lock()
	o.active--
	if o.active == 0 && o.cond != nil {
		o.cond.Broadcast()
	}
	o.mu.Unlock()
}

// wait blocks until no detached operation is left.
func (o *opTracker) wait() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.cond == nil {
		o.cond = sync.NewCond(&o.mu)
	}
	for o.active > 0 {
		o.cond.Wait()
	}
}

// callBudget is the store-side bound of one operation. It is the store's own
// value, never derived from a caller (§10 判定; ADR-054 决策 7).
func (s *redisStore) callBudget() time.Duration {
	if s.cfg.CallTimeout > 0 {
		return s.cfg.CallTimeout
	}
	return DefaultCallTimeout
}

// boundCtx detaches ctx from its caller and gives the operation the store's
// own budget. storeCall reuses an already-bound context before reaching here,
// so a nested operation shares the outer bound instead of stacking a second.
func (s *redisStore) boundCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	base := context.Background()
	if ctx != nil {
		base = context.WithoutCancel(ctx)
	}
	return context.WithTimeout(context.WithValue(base, opBoundKey{}, struct{}{}), s.callBudget())
}

// storeCall runs one store operation on the store's own bound. The caller's
// context bounds only its wait: when it ends first the caller gets its own
// error and the operation keeps running to its bound, so its outcome still
// reaches mark (§10 判定; ADR-054 决策 4).
func storeCall[T any](s *redisStore, ctx context.Context, op func(context.Context) (T, error)) (T, error) {
	var zero T
	if ctx != nil {
		if _, ok := ctx.Value(opBoundKey{}).(struct{}); ok {
			return op(ctx)
		}
		// A caller that is already done never started this operation, so
		// there is nothing to detach and no write to perform.
		if err := ctx.Err(); err != nil {
			return zero, err
		}
	}
	opCtx, cancel := s.boundCtx(ctx)
	// Without a cancellable caller there is no wait to end, so the operation
	// runs inline and no goroutine is spent on it.
	if ctx == nil || ctx.Done() == nil {
		defer cancel()
		return op(opCtx)
	}
	type result struct {
		value T
		err   error
	}
	done := make(chan result, 1)
	s.detached.begin()
	go func() {
		defer s.detached.end()
		defer cancel()
		value, err := op(opCtx)
		done <- result{value: value, err: err}
	}()
	select {
	case r := <-done:
		return r.value, r.err
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

func storeCall0(s *redisStore, ctx context.Context, op func(context.Context) error) error {
	_, err := storeCall(s, ctx, func(opCtx context.Context) (struct{}, error) {
		return struct{}{}, op(opCtx)
	})
	return err
}

func storeCall2[A, B any](s *redisStore, ctx context.Context, op func(context.Context) (A, B, error)) (A, B, error) {
	type pair struct {
		first  A
		second B
	}
	r, err := storeCall(s, ctx, func(opCtx context.Context) (pair, error) {
		first, second, err := op(opCtx)
		return pair{first: first, second: second}, err
	})
	return r.first, r.second, err
}

// The public face delegates to the implementation under the boundary above.
// The availability reads stay direct: they report a state, they do not touch
// the store.

func (s *redisStore) PutEntry(ctx context.Context, ws, taskID string, e registry.TaskEntry) error {
	return storeCall0(s, ctx, func(ctx context.Context) error { return s.putEntry(ctx, ws, taskID, e) })
}

func (s *redisStore) GetEntry(ctx context.Context, ws, taskID string) (registry.TaskEntry, bool, error) {
	return storeCall2(s, ctx, func(ctx context.Context) (registry.TaskEntry, bool, error) {
		return s.getEntry(ctx, ws, taskID)
	})
}

func (s *redisStore) ListEntries(ctx context.Context) ([]registry.TaskEntry, error) {
	return storeCall(s, ctx, func(ctx context.Context) ([]registry.TaskEntry, error) { return s.listEntries(ctx) })
}

func (s *redisStore) PutSettled(ctx context.Context, ws, taskID string, e registry.TaskEntry) error {
	return storeCall0(s, ctx, func(ctx context.Context) error { return s.putSettled(ctx, ws, taskID, e) })
}

func (s *redisStore) GetSettled(ctx context.Context, ws, taskID string) (registry.TaskEntry, bool, error) {
	return storeCall2(s, ctx, func(ctx context.Context) (registry.TaskEntry, bool, error) {
		return s.getSettled(ctx, ws, taskID)
	})
}

func (s *redisStore) ListSettled(ctx context.Context) ([]registry.TaskEntry, error) {
	return storeCall(s, ctx, func(ctx context.Context) ([]registry.TaskEntry, error) { return s.listSettled(ctx) })
}

func (s *redisStore) DeleteSettled(ctx context.Context, ws, taskID string) error {
	return storeCall0(s, ctx, func(ctx context.Context) error { return s.deleteSettled(ctx, ws, taskID) })
}

func (s *redisStore) Revoke(ctx context.Context, ws, hash string, validUntil time.Time) error {
	return storeCall0(s, ctx, func(ctx context.Context) error { return s.revoke(ctx, ws, hash, validUntil) })
}

func (s *redisStore) IsRevoked(ctx context.Context, ws, hash string) (bool, error) {
	return storeCall(s, ctx, func(ctx context.Context) (bool, error) { return s.isRevoked(ctx, ws, hash) })
}

func (s *redisStore) ListRevoked(ctx context.Context) (map[string]map[string]time.Time, error) {
	return storeCall(s, ctx, func(ctx context.Context) (map[string]map[string]time.Time, error) {
		return s.listRevoked(ctx)
	})
}

func (s *redisStore) Ping(ctx context.Context) error {
	return storeCall0(s, ctx, func(ctx context.Context) error { return s.ping(ctx) })
}

func (s *redisStore) SetEntry(ctx context.Context, ws, taskID string, e registry.TaskEntry) error {
	return storeCall0(s, ctx, func(ctx context.Context) error { return s.setEntry(ctx, ws, taskID, e) })
}

func (s *redisStore) MergeEntry(ctx context.Context, ws, taskID string, p registry.EntryPatch) error {
	return storeCall0(s, ctx, func(ctx context.Context) error { return s.mergeEntry(ctx, ws, taskID, p) })
}

func (s *redisStore) EnqueueEntry(ctx context.Context, ws, taskID string, e registry.TaskEntry) (bool, error) {
	return storeCall(s, ctx, func(ctx context.Context) (bool, error) { return s.enqueueEntry(ctx, ws, taskID, e) })
}

func (s *redisStore) PickupDelivery(ctx context.Context, ws, taskID string, window time.Duration) (Pickup, error) {
	return storeCall(s, ctx, func(ctx context.Context) (Pickup, error) {
		return s.pickupDelivery(ctx, ws, taskID, window)
	})
}

func (s *redisStore) ConfirmStart(ctx context.Context, ws, taskID string, at time.Time, advance bool) (registry.TaskEntry, bool, error) {
	return storeCall2(s, ctx, func(ctx context.Context) (registry.TaskEntry, bool, error) {
		return s.confirmStart(ctx, ws, taskID, at, advance)
	})
}

func (s *redisStore) CommitDelivering(ctx context.Context, ws, taskID string, at time.Time, payload []byte, window time.Duration) (registry.TaskEntry, bool, error) {
	return storeCall2(s, ctx, func(ctx context.Context) (registry.TaskEntry, bool, error) {
		return s.commitDelivering(ctx, ws, taskID, at, payload, window)
	})
}

func (s *redisStore) CommitJobCreated(ctx context.Context, ws, taskID string, at time.Time) (registry.TaskEntry, bool, error) {
	return storeCall2(s, ctx, func(ctx context.Context) (registry.TaskEntry, bool, error) {
		return s.commitJobCreated(ctx, ws, taskID, at)
	})
}

func (s *redisStore) CommitDaemonRegistered(ctx context.Context, ws, taskID string, at time.Time) (registry.TaskEntry, bool, error) {
	return storeCall2(s, ctx, func(ctx context.Context) (registry.TaskEntry, bool, error) {
		return s.commitDaemonRegistered(ctx, ws, taskID, at)
	})
}

func (s *redisStore) CommitTerminal(ctx context.Context, ws, taskID string, result registry.Result, reason string, completedAt, drainDeadline time.Time) (registry.TaskEntry, bool, error) {
	return storeCall2(s, ctx, func(ctx context.Context) (registry.TaskEntry, bool, error) {
		return s.commitTerminal(ctx, ws, taskID, result, reason, completedAt, drainDeadline)
	})
}

func (s *redisStore) CommitDecidedReport(ctx context.Context, ws, taskID string, body []byte) (registry.TaskEntry, bool, error) {
	return storeCall2(s, ctx, func(ctx context.Context) (registry.TaskEntry, bool, error) {
		return s.commitDecidedReport(ctx, ws, taskID, body)
	})
}

func (s *redisStore) SettleNoResult(ctx context.Context, ws, taskID string, expected registry.Result) (registry.TaskEntry, bool, error) {
	return storeCall2(s, ctx, func(ctx context.Context) (registry.TaskEntry, bool, error) {
		return s.settleNoResult(ctx, ws, taskID, expected)
	})
}

func (s *redisStore) DeleteEntry(ctx context.Context, ws, taskID string) error {
	return storeCall0(s, ctx, func(ctx context.Context) error { return s.deleteEntry(ctx, ws, taskID) })
}

func (s *redisStore) DropQueue(ctx context.Context, ws, taskID string) error {
	return storeCall0(s, ctx, func(ctx context.Context) error { return s.dropQueue(ctx, ws, taskID) })
}

func (s *redisStore) QueueDepth(ctx context.Context) (int64, error) {
	return storeCall(s, ctx, func(ctx context.Context) (int64, error) { return s.queueDepth(ctx) })
}

func (s *redisStore) GetEntryByJob(ctx context.Context, jobName string) (registry.TaskEntry, bool, error) {
	return storeCall2(s, ctx, func(ctx context.Context) (registry.TaskEntry, bool, error) {
		return s.getEntryByJob(ctx, jobName)
	})
}

func (s *redisStore) LockTask(ctx context.Context, ws, taskID, owner string, ttl time.Duration) (bool, error) {
	return storeCall(s, ctx, func(ctx context.Context) (bool, error) {
		return s.lockTask(ctx, ws, taskID, owner, ttl)
	})
}

func (s *redisStore) UnlockTask(ctx context.Context, ws, taskID, owner string) error {
	return storeCall0(s, ctx, func(ctx context.Context) error { return s.unlockTask(ctx, ws, taskID, owner) })
}

func (s *redisStore) ReserveSlot(ctx context.Context, ws string, limit int) (string, bool, error) {
	return storeCall2(s, ctx, func(ctx context.Context) (string, bool, error) {
		return s.reserveSlot(ctx, ws, limit)
	})
}

func (s *redisStore) ConsumeSlotReservation(ctx context.Context, ws, id string) error {
	return storeCall0(s, ctx, func(ctx context.Context) error { return s.consumeSlotReservation(ctx, ws, id) })
}

func (s *redisStore) ReleaseSlot(ctx context.Context, ws, id string) error {
	return storeCall0(s, ctx, func(ctx context.Context) error { return s.releaseSlot(ctx, ws, id) })
}

func (s *redisStore) ReleaseEntry(ctx context.Context, ws string) error {
	return storeCall0(s, ctx, func(ctx context.Context) error { return s.releaseEntry(ctx, ws) })
}

func (s *redisStore) SlotSnapshot(ctx context.Context, ws string) (int64, int64, error) {
	return storeCall2(s, ctx, func(ctx context.Context) (int64, int64, error) {
		return s.slotSnapshot(ctx, ws)
	})
}

func (s *redisStore) CompareAndSetSlots(ctx context.Context, ws string, expected, next int64) (bool, error) {
	return storeCall(s, ctx, func(ctx context.Context) (bool, error) {
		return s.compareAndSetSlots(ctx, ws, expected, next)
	})
}

func (s *redisStore) ResetSlots(ctx context.Context, ws string, n int64) (int64, int64, error) {
	return storeCall2(s, ctx, func(ctx context.Context) (int64, int64, error) {
		return s.resetSlots(ctx, ws, n)
	})
}

func (s *redisStore) Slots(ctx context.Context, ws string) (int64, error) {
	return storeCall(s, ctx, func(ctx context.Context) (int64, error) { return s.slots(ctx, ws) })
}

func (s *redisStore) AcquireLeadership(ctx context.Context, owner string, ttl time.Duration) (bool, error) {
	return storeCall(s, ctx, func(ctx context.Context) (bool, error) {
		return s.acquireLeadership(ctx, owner, ttl)
	})
}

func (s *redisStore) RenewLeadership(ctx context.Context, owner string, ttl time.Duration) (bool, error) {
	return storeCall(s, ctx, func(ctx context.Context) (bool, error) {
		return s.renewLeadership(ctx, owner, ttl)
	})
}

func (s *redisStore) ReleaseLeadership(ctx context.Context, owner string) error {
	return storeCall0(s, ctx, func(ctx context.Context) error { return s.releaseLeadership(ctx, owner) })
}

func (s *redisStore) LeadershipHolder(ctx context.Context) (string, error) {
	return storeCall(s, ctx, func(ctx context.Context) (string, error) { return s.leadershipHolder(ctx) })
}

func (s *redisStore) HeartbeatInstance(ctx context.Context, id string, ttl time.Duration) error {
	return storeCall0(s, ctx, func(ctx context.Context) error { return s.heartbeatInstance(ctx, id, ttl) })
}

func (s *redisStore) ListInstances(ctx context.Context) ([]string, error) {
	return storeCall(s, ctx, func(ctx context.Context) ([]string, error) { return s.listInstances(ctx) })
}

func (s *redisStore) PutDeclaration(ctx context.Context, ws string, raw []byte) error {
	return storeCall0(s, ctx, func(ctx context.Context) error { return s.putDeclaration(ctx, ws, raw) })
}

func (s *redisStore) GetDeclaration(ctx context.Context, ws string) ([]byte, bool, error) {
	return storeCall2(s, ctx, func(ctx context.Context) ([]byte, bool, error) {
		return s.getDeclaration(ctx, ws)
	})
}

func (s *redisStore) PutProbeSession(ctx context.Context, jobName string, raw []byte, ttl time.Duration) error {
	return storeCall0(s, ctx, func(ctx context.Context) error { return s.putProbeSession(ctx, jobName, raw, ttl) })
}

func (s *redisStore) CASProbeSession(ctx context.Context, jobName string, expected, next []byte, ttl time.Duration) (bool, error) {
	return storeCall(s, ctx, func(ctx context.Context) (bool, error) {
		return s.casProbeSession(ctx, jobName, expected, next, ttl)
	})
}

func (s *redisStore) GetProbeSession(ctx context.Context, jobName string) ([]byte, bool, error) {
	return storeCall2(s, ctx, func(ctx context.Context) ([]byte, bool, error) {
		return s.getProbeSession(ctx, jobName)
	})
}

func (s *redisStore) DeleteProbeSession(ctx context.Context, jobName string) error {
	return storeCall0(s, ctx, func(ctx context.Context) error { return s.deleteProbeSession(ctx, jobName) })
}

func (s *redisStore) ListProbeSessions(ctx context.Context) (map[string][]byte, error) {
	return storeCall(s, ctx, func(ctx context.Context) (map[string][]byte, error) {
		return s.listProbeSessions(ctx)
	})
}

func (s *redisStore) PutSessionNode(ctx context.Context, ws, key string, raw []byte, ttl time.Duration) error {
	return storeCall0(s, ctx, func(ctx context.Context) error { return s.putSessionNode(ctx, ws, key, raw, ttl) })
}

func (s *redisStore) GetSessionNode(ctx context.Context, ws, key string) (registry.SharedSessionNode, bool, error) {
	return storeCall2(s, ctx, func(ctx context.Context) (registry.SharedSessionNode, bool, error) {
		return s.getSessionNode(ctx, ws, key)
	})
}

func (s *redisStore) PutCatalogSlot(ctx context.Context, runtimeID string, raw []byte, ttl time.Duration) error {
	return storeCall0(s, ctx, func(ctx context.Context) error { return s.putCatalogSlot(ctx, runtimeID, raw, ttl) })
}

func (s *redisStore) GetCatalogSlot(ctx context.Context, runtimeID string) ([]byte, bool, error) {
	return storeCall2(s, ctx, func(ctx context.Context) ([]byte, bool, error) {
		return s.getCatalogSlot(ctx, runtimeID)
	})
}

func (s *redisStore) DeleteCatalogSlot(ctx context.Context, runtimeID string) error {
	return storeCall0(s, ctx, func(ctx context.Context) error { return s.deleteCatalogSlot(ctx, runtimeID) })
}
