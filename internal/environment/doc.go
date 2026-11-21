// Package environment owns the registered rsbxd execution environments: the
// operator's target record, the verification facts, the slot ledger and the
// retirement tombstones. An environment is a remote machine identity, not a
// Kubernetes node label or a Pod affinity: the package never reads a Node
// object and selection uses only the registered target facts.
package environment
