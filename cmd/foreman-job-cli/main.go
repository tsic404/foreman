// Command foreman-job-cli is the reference CLI preparation adapter. It fetches
// exactly one frozen artifact from the Foreman proxy and answers its verified
// local path. A deployment that keeps its own downloader replaces this binary;
// the supervisor only relies on the answer's observable facts.
package main

import (
	"os"

	"github.com/tsic404/foreman/internal/jobexec"
)

func main() {
	os.Exit(jobexec.Main(map[string]jobexec.PhaseFunc{"prepare": jobexec.Prepare}))
}
