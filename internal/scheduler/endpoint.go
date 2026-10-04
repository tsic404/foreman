package scheduler

// Endpoint identifies a task lifecycle callback forwarded to the server
// (contract §2). The full routing table lives in the proxy module; the enum
// is shared so scheduler and recovery can name endpoints without importing
// protocol details.
type Endpoint string

// Task lifecycle endpoints (contract §2, exhaustive).
const (
	EPStart        Endpoint = "start"
	EPProgress     Endpoint = "progress"
	EPMessages     Endpoint = "messages"
	EPUsage        Endpoint = "usage"
	EPComplete     Endpoint = "complete"
	EPFail         Endpoint = "fail"
	EPCancelAck    Endpoint = "cancel-ack"
	EPSession      Endpoint = "session"
	EPWaitLocalDir Endpoint = "wait-local-directory"
)

// IsTerminal reports whether ep settles the task on the server
// (C9 complete / C10 fail / C11 cancel-ack).
func (ep Endpoint) IsTerminal() bool {
	return ep == EPComplete || ep == EPFail || ep == EPCancelAck
}

// TerminalResult maps a terminal endpoint to its result classification.
func (ep Endpoint) TerminalResult() string {
	switch ep {
	case EPComplete:
		return "completed"
	case EPFail:
		return "failed"
	case EPCancelAck:
		return "cancelled"
	}
	return ""
}
