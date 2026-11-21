// Package scope defines the partition key shared by the runtime face.
//
// A Multica workspace id is only unique inside its source server: two
// registered servers can expose the same workspace id, and the design treats
// (serverId, workspaceId) as the partition (02-contracts V2-C03/C06,
// 04-architecture §3.1). The empty source keeps the single-source form's
// historical key unchanged, so existing rows and tests remain valid.
package scope

import "strings"

const separator = "\x00"

// Key returns the internal partition key of one source and workspace.
func Key(sourceID, workspaceID string) string {
	if sourceID == "" {
		return workspaceID
	}
	return sourceID + separator + workspaceID
}

// Split divides a partition key back into its source and workspace. A key
// without a separator is the legacy single-source form.
func Split(key string) (sourceID, workspaceID string) {
	if before, after, found := strings.Cut(key, separator); found {
		return before, after
	}
	return "", key
}
