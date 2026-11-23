// Command foreman-job-register is the reference registration observation
// adapter: it answers the daemon identity, runtime and CLI version Foreman
// actually recorded, and never reports a registration it did not observe.
package main

import (
	"os"

	"github.com/tsic404/foreman/internal/jobexec"
)

func main() {
	os.Exit(jobexec.Main(map[string]jobexec.PhaseFunc{"register": jobexec.Register}))
}
