// Command rsbx-remote is the reference session adapter: it opens the frozen
// rsbx session the attempt addresses and answers whether the remote close is
// confirmed. The carrier, its arguments and its configuration format are this
// adapter's own business; the answer's facts are what the supervisor reads.
package main

import (
	"os"

	"github.com/tsic404/foreman/internal/jobexec"
)

func main() {
	os.Exit(jobexec.Main(map[string]jobexec.PhaseFunc{
		"ready": jobexec.Ready,
		"close": jobexec.Close,
	}))
}
