package attempt_test

import (
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/attempt"
	"github.com/felinics/twilight/agentcore/session/extension"
)

// EXT-COD-1: the module's one event is canonical round-trip stable, and the
// index refuses a Run bound twice or an attempt out of order (ATT-1).
func TestAttemptModule(t *testing.T) {
	reg, err := extension.BuildRegistry(attempt.Module)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := reg.Encode(attempt.TypeStarted, attempt.StartedPayload{TurnID: "t1", RunID: "r1", Attempt: 1})
	if err != nil {
		t.Fatal(err)
	}
	if wire.String() != `{"attempt":1,"runId":"r1","turnId":"t1","v":1}` {
		t.Fatalf("wire = %s", wire)
	}
	decoded, err := reg.Decode(session.Event{Type: attempt.TypeStarted, Payload: wire})
	if err != nil || decoded.Value != (attempt.StartedPayload{TurnID: "t1", RunID: "r1", Attempt: 1}) {
		t.Fatalf("decode = %+v %v", decoded.Value, err)
	}
	if _, err := reg.Encode(attempt.TypeStarted, attempt.StartedPayload{TurnID: "t1", RunID: "r1"}); err == nil {
		t.Fatal("attempt 0 encoded")
	}

	fold := func(events ...attempt.StartedPayload) (attempt.Index, error) {
		state, err := attempt.IndexProjection.Initial()
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range events {
			if state, err = attempt.IndexProjection.Apply(state, extension.DecodedEvent{Value: p}); err != nil {
				return attempt.Index{}, err
			}
		}
		return state.(attempt.Index), nil
	}
	first, second := attempt.StartedPayload{TurnID: "t1", RunID: "r1", Attempt: 1}, attempt.StartedPayload{TurnID: "t1", RunID: "r2", Attempt: 2}
	cases := []struct {
		name    string
		events  []attempt.StartedPayload
		wantErr string
	}{
		{"two attempts of one turn", []attempt.StartedPayload{first, second}, ""},
		{"run bound twice", []attempt.StartedPayload{first, {TurnID: "t2", RunID: "r1", Attempt: 1}}, "already attempt 1 of turn t1"},
		{"ordinal skipped", []attempt.StartedPayload{first, {TurnID: "t1", RunID: "r3", Attempt: 3}}, "out of order, want 2"},
		{"retry before first", []attempt.StartedPayload{second}, "out of order, want 1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			index, err := fold(tc.events...)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("fold = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if rec, ok := index.Of("r2"); !ok || rec.TurnID != "t1" || rec.Attempt != 2 {
				t.Fatalf("of r2 = %+v %v", rec, ok)
			}
			if got := index.Attempts("t1"); len(got) != 2 || got[0].RunID != "r1" || got[1].RunID != "r2" {
				t.Fatalf("attempts = %+v", got)
			}
		})
	}
}
