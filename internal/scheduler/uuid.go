package scheduler

import (
	"crypto/sha1"
	"fmt"
)

// jobRuntimeNamespace is the fixed UUIDv5 namespace for job runtime IDs
// (contract §3.1: job_runtime_id = uuid5(ns_job_runtime, job_name)). It must
// never change: rebuilt entries carry the ID in Job annotations and a running
// daemon keeps presenting it.
var jobRuntimeNamespace = [16]byte{
	0x6f, 0x3d, 0x9b, 0x12, 0x4a, 0x47, 0x5e, 0x88,
	0xb3, 0x21, 0xd7, 0x0c, 0x54, 0xf9, 0x2e, 0xa6,
}

// jobRuntimeID derives the deterministic fake runtime ID for a Job.
func jobRuntimeID(jobName string) string {
	sum := sha1.Sum(append(jobRuntimeNamespace[:], []byte(jobName)...))
	sum[6] = (sum[6] & 0x0f) | 0x50 // version 5
	sum[8] = (sum[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", sum[0:4], sum[4:6], sum[6:8], sum[8:10], sum[10:16])
}
