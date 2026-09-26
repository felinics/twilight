package checkpointtest

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/checkpoint"
)

// Factory builds a fresh, empty Store for one subtest.
type Factory func(t *testing.T) checkpoint.Store

// Run executes the suite.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("positions", func(t *testing.T) { testPositions(t, factory(t)) })
}

// A checkpoint moves forward, stays where it is on a repeated save, refuses
// to move backwards, and (consumer, ledger) pairs are independent.
func testPositions(t *testing.T, cp checkpoint.Store) {
	ctx := context.Background()
	if next, ok, err := cp.Load(ctx, "relay", "session/s1"); err != nil || ok || next != 0 {
		t.Fatalf("unsaved checkpoint = %d %v %v, want 0 false", next, ok, err)
	}
	steps := []struct {
		name    string
		next    uint64
		wantErr error
		wantAt  uint64
	}{
		{"first save", 3, nil, 3},
		{"forward", 7, nil, 7},
		{"same position is idempotent", 7, nil, 7},
		{"backwards is refused", 5, checkpoint.ErrRewind, 7},
	}
	for _, s := range steps {
		t.Run(s.name, func(t *testing.T) {
			if err := cp.Save(ctx, "relay", "session/s1", s.next); !errors.Is(err, s.wantErr) {
				t.Fatalf("save = %v, want %v", err, s.wantErr)
			}
			if next, ok, err := cp.Load(ctx, "relay", "session/s1"); err != nil || !ok || next != s.wantAt {
				t.Fatalf("load = %d %v %v, want %d", next, ok, err, s.wantAt)
			}
		})
	}
	if err := cp.Save(ctx, "projection", "session/s1", 1); err != nil {
		t.Fatal(err)
	}
	if err := cp.Save(ctx, "relay", "session/s2", 1); err != nil {
		t.Fatal(err)
	}
	if next, _, _ := cp.Load(ctx, "relay", "session/s1"); next != 7 {
		t.Fatalf("relay/s1 moved to %d", next)
	}
}
