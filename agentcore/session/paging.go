package session

import (
	"github.com/felinics/twilight/agentcore/ledger"
)

// IndexWithin converts a commit offset to an index into a slice of n items,
// clamping to n so a caller can slice from it without checking bounds.
func IndexWithin(off ledger.CommitSeq, n int) int {
	if n <= 0 || uint64(off) >= uint64(n) {
		return max(n, 0)
	}
	return int(off) //nolint:gosec // off < n <= MaxInt
}

// AtLimit reports whether a page of n items has reached limit; a zero limit
// is no limit.
func AtLimit(n int, limit uint32) bool {
	return limit > 0 && n >= 0 && uint64(n) >= uint64(limit)
}
