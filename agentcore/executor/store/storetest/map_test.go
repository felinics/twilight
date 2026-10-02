package storetest

import (
	"sync"
	"testing"
	"time"
)

func TestMapConformance(t *testing.T) {
	Run(t, func(*testing.T) Fixture {
		var mu sync.Mutex
		now := time.Unix(2_000_000, 0)
		m := NewMap(func() time.Time { mu.Lock(); defer mu.Unlock(); return now })
		return Fixture{Store: m, Advance: func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }}
	})
}
