// Package catalog is the multi-source directory behind the console: one
// stable id per registered Multica server, per-source sync generations, the
// workspace/project/agent/runtime rows, binding write-back confirmed by
// read-back, and the overview read model.
//
// Design: 01-scope V2-G1, 02-contracts V2-C01/V2-C02, 04-architecture §2,
// §3.1, §3.2 and §8. Identity is (serverId, workspaceId, objectId); a failed
// sync keeps its own previous generation; only a read-back confirms a write.
package catalog
