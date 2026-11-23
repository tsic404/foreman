// Command foreman-job-runner is the reference dispatch adapter: it answers
// whether this attempt's payload arrived at the remote Agent, and keeps an
// arrived-but-interrupted dispatch distinct from a payload that never arrived.
package main

import (
	"os"

	"github.com/tsic404/foreman/internal/jobexec"
)

func main() {
	os.Exit(jobexec.Main(map[string]jobexec.PhaseFunc{"run": jobexec.Run}))
}
