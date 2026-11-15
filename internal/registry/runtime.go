package registry

import "time"

// Runtime is one long-lived multica daemon registered to Foreman. In v2 a
// daemon is a first-class peer that stays alive across tasks (a "job" in v1
// was one disposable pod). It declares the runtimes it can serve and
// heartbeats to stay online; the dispatcher assigns tasks to the best online
// daemon of the task's runtime.
type Runtime struct {
	RuntimeID string // the daemon's self-chosen runtime id (stable across tasks)
	DaemonID  string // the daemon's identity (its token subject)
	Provider  string // runtime type the daemon serves (e.g. "omp")
	ProfileID string // runtime profile id ("" for builtin)
	Name      string // display name
	WorkspaceID string

	Online          bool
	RegisteredAt    time.Time
	LastHeartbeatAt time.Time
	// Sessions is the set of session ids this daemon currently holds on disk,
	// learned from terminal reports (session affinity, ADR-015); a claimed
	// task whose prior_session_id is here prefers this daemon.
	Sessions map[string]bool
	// Busy reports the number of tasks currently running on this daemon; the
	// dispatcher balances across daemons by this count.
	Busy int
}

// HoldsSession reports whether the daemon already has sessionID's files.
func (r Runtime) HoldsSession(sessionID string) bool {
	return sessionID != "" && r.Sessions[sessionID]
}