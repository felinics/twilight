package executor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/run/effect"
)

// RUN-EXE-12: the hub stamps generation and sequence, replays after a
// sequence, voids a generation with a reset, closes with an end, and bounds
// what it keeps.
func TestProgressHub(t *testing.T) {
	ctx := context.Background()
	key := effect.AssignmentKey{Session: "s", RunID: "r", Effect: "e"}
	other := effect.AssignmentKey{Session: "s", RunID: "r", Effect: "x"}
	h := NewProgressHub(8)
	pub := func(k effect.AssignmentKey, text string) {
		h.Publish(ctx, effect.ProgressFrame{Key: k, Kind: effect.ProgressTextDelta, Payload: json.RawMessage(`"` + text + `"`)})
	}
	collect := func(k effect.AssignmentKey, after uint64, limit int) ([]effect.ProgressFrame, error) {
		var got []effect.ProgressFrame
		ctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		err := h.Progress(ctx, k, after, func(f effect.ProgressFrame) bool {
			got = append(got, f)
			return len(got) < limit
		})
		return got, err
	}
	pub(key, "a")
	pub(key, "b")
	h.Reset(key)
	pub(key, "c")
	h.End(key)
	cases := []struct {
		name    string
		after   uint64
		limit   int
		wantSeq []uint64
		wantGen []int
	}{
		{name: "full replay ends with the end frame", after: 0, limit: 10, wantSeq: []uint64{1, 2, 3, 4, 5}, wantGen: []int{1, 1, 2, 2, 2}},
		{name: "after the reset", after: 3, limit: 10, wantSeq: []uint64{4, 5}, wantGen: []int{2, 2}},
		{name: "subscriber stops early", after: 0, limit: 2, wantSeq: []uint64{1, 2}, wantGen: []int{1, 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := collect(key, tc.after, tc.limit)
			if err != nil || len(got) != len(tc.wantSeq) {
				t.Fatalf("frames = %+v, %v; want %v", got, err, tc.wantSeq)
			}
			for i := range got {
				if got[i].Sequence != tc.wantSeq[i] || got[i].Generation != tc.wantGen[i] {
					t.Fatalf("frame %d = seq %d gen %d, want seq %d gen %d", i, got[i].Sequence, got[i].Generation, tc.wantSeq[i], tc.wantGen[i])
				}
			}
			if tc.after == 0 && tc.limit >= 10 {
				if got[2].Kind != effect.ProgressReset || got[len(got)-1].Kind != effect.ProgressEnd {
					t.Fatalf("kinds = %v, want a reset third and an end last", got)
				}
			}
		})
	}
	// Publishing after the end is dropped; an unknown key waits until ctx ends.
	pub(key, "late")
	if got, _ := collect(key, 5, 10); len(got) != 0 {
		t.Fatalf("frames after end = %+v, want none", got)
	}
	if _, err := collect(other, 0, 10); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unknown key = %v, want deadline (the hub waits for it)", err)
	}
	// The window bounds what is kept: with a window of 4, six frames and
	// the end replay as the newest four.
	h = NewProgressHub(4)
	for _, s := range []string{"1", "2", "3", "4", "5", "6"} {
		pub(other, s)
	}
	h.End(other)
	got, _ := collect(other, 0, 10)
	if len(got) != 4 || got[0].Sequence != 4 {
		t.Fatalf("windowed replay = %d frames from seq %d, want 4 from 4", len(got), got[0].Sequence)
	}
}
