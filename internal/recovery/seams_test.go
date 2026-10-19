package recovery

import (
	"github.com/tsic404/foreman/internal/scheduler"
)

// The wiring contract between the two modules, asserted at compile time:
// the scheduler delegates its Reconcile seam to the recovery reconciler and
// supplies the settlement primitives the reconciler drives. The terminal
// report queue is the scheduler's own (ADR-031) and is drained through that
// same settlement surface.
var (
	_ scheduler.Reconciler         = (*Reconciler)(nil)
	_ scheduler.PendingReportQueue = (*scheduler.PendingReports)(nil)
	_ Settler                      = (*scheduler.Scheduler)(nil)
)
