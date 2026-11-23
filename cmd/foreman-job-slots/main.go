// Command foreman-job-slots is the reference reservation adapter: it reads the
// shared ledger before any session opens and commits each Job-side transition
// to it. Capacity returns only when the close was proven.
package main

import (
	"os"

	"github.com/tsic404/foreman/internal/jobexec"
)

func main() {
	os.Exit(jobexec.Main(map[string]jobexec.PhaseFunc{
		"status":   jobexec.Status,
		"begin":    jobexec.Begin,
		"takeover": jobexec.TakeOver,
		"bind":     jobexec.Bind,
		"started":  jobexec.MarkStarted,
		"release":  jobexec.Release,
	}))
}
