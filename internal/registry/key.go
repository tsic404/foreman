package registry

import "github.com/tsic404/foreman/internal/scope"

// TaskKey names one attempt in the local index. The upstream task id alone is
// not unique across registered servers, so the source and workspace partition
// travels with it (02-contracts V2-C02/C03). An entry without either keeps the
// historical bare task-id key.
func TaskKey(sourceID, workspaceID, taskID string) string {
	if sourceID == "" && workspaceID == "" {
		return taskID
	}
	return scope.Key(sourceID, workspaceID) + "\x00" + taskID
}

// Key is the entry's own index key.
func (e TaskEntry) Key() string {
	return TaskKey(e.SourceID, e.WorkspaceID, e.TaskID)
}

// keyInScope names one task under an already-composed partition key.
func keyInScope(scopeKey, taskID string) string {
	if scopeKey == "" {
		return taskID
	}
	return scopeKey + "\x00" + taskID
}
