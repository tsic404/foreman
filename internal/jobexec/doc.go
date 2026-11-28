// Package jobexec is the reference implementation of the fixed Job's five
// deployment adapters. The supervisor starts each adapter as one short-lived
// process: argv[1] names the phase, FOREMAN_JOB_REQUEST carries one JSON
// request, and the answer is one JSON document on stdout.
//
// The process, argument and environment conventions are implementation
// details (02-contracts.md V2-C29): a deployment may supply its own adapters,
// as long as they answer the same observable facts. This package is that
// default. It never reports an unobserved fact: an adapter that cannot prove
// an answer exits nonzero, and only a session that provably does not exist is
// answered with ExitSessionNotCreated.
package jobexec
