package attempt

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"
)

var claimFallback atomic.Uint64

// NewClaimID mints one execution lease. Every Run of the fixed Job uses a
// fresh value, so two instances of the same attempt cannot share it.
func NewClaimID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err == nil {
		return hex.EncodeToString(raw[:])
	}
	return fmt.Sprintf("claim-%d-%d", time.Now().UnixNano(), claimFallback.Add(1))
}
