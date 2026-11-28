package jobexec

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// The attempt's CLI download budget is one number shared by both artifacts,
// their retries and every helper process. Each prepare invocation is a fresh
// process, so the count lives beside the session state and is updated under a
// file lock: a second helper reads what the first one spent instead of
// starting from zero.
const (
	budgetFile = "budget.json"
	budgetLock = "budget.lock"
)

type attemptBudget struct {
	Spent int64 `json:"spent"`
}

// reserveAttemptBudget takes the attempt's budget lock and answers the bytes
// still available. The caller runs the download and then commits the bytes it
// actually consumed; the lock stays held until release so two helpers cannot
// both spend the same remainder.
func reserveAttemptBudget(dir string, limit int64) (remaining int64, commit func(int64) error, release func(), err error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return 0, nil, nil, fmt.Errorf("create the attempt state directory: %w", err)
	}
	lock, err := os.OpenFile(filepath.Join(dir, budgetLock), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("open the attempt budget lock: %w", err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		_ = lock.Close()
		return 0, nil, nil, fmt.Errorf("lock the attempt budget: %w", err)
	}
	release = func() {
		_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		_ = lock.Close()
	}
	state := attemptBudget{}
	body, readErr := os.ReadFile(filepath.Join(dir, budgetFile))
	switch {
	case readErr == nil:
		if err := json.Unmarshal(body, &state); err != nil {
			release()
			return 0, nil, nil, fmt.Errorf("decode the attempt budget: %w", err)
		}
	case errors.Is(readErr, os.ErrNotExist):
	default:
		release()
		return 0, nil, nil, fmt.Errorf("read the attempt budget: %w", readErr)
	}
	remaining = limit - state.Spent
	if remaining < 0 {
		remaining = 0
	}
	commit = func(consumed int64) error {
		if consumed <= 0 {
			return nil
		}
		state.Spent += consumed
		raw, err := json.Marshal(state)
		if err != nil {
			return fmt.Errorf("encode the attempt budget: %w", err)
		}
		return writeFileAtomic(filepath.Join(dir, budgetFile), raw, 0o600)
	}
	return remaining, commit, release, nil
}
