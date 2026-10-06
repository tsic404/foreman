package recovery

import (
	"github.com/tsic404/foreman/internal/scheduler"
)

// The wiring contract between the two modules, asserted at compile time:
// the scheduler delegates its Reconcile seam to the recovery reconciler and
// supplies the settlement primitives the reconciler drives.
var (
	_ scheduler.Reconciler     = (*Reconciler)(nil)
	_ scheduler.PendingReports = (*PendingReports)(nil)
	_ Settler                  = (*scheduler.Scheduler)(nil)
)
