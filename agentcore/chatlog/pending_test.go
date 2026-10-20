package chatlog

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"
)

func assertPendingMatchesSurface(t *testing.T, pending Pending, surface Surface) {
	t.Helper()
	got, want := pending.SubmittedInputs(), surface.SubmittedInputs()
	if len(got) != len(want) {
		t.Fatalf("pending=%+v surface=%+v", got, want)
	}
	for i := range want {
		if got[i].ID != want[i].ID || got[i].Digest != want[i].Digest || got[i].Placement != want[i].Placement {
			t.Fatalf("pending[%d]=%+v surface=%+v", i, got[i], want[i])
		}
	}
}

func TestPendingMatchesSurfaceAcrossLifecycleAndCheckpoint(t *testing.T) {
	r := registry(t)
	scope, err := r.ScopeFor(PendingProjectionID, PendingProjection.Version)
	if err != nil {
		t.Fatal(err)
	}
	surfaceScope, err := r.ScopeFor(SurfaceProjectionID, SurfaceProjection.Version)
	if err != nil {
		t.Fatal(err)
	}
	pending, _ := PendingProjection.Initial()
	surface, _ := SurfaceProjection.Initial()
	steps := []step{
		{TypeInputSubmitted, InputSubmittedPayload{InputID: "z", Content: jsonstable.MustParse(`{"text":"hi"}`), Placement: PlacementNextTurn, SubmittedAtUnixMilli: 1}},
		{TypeInputSubmitted, InputSubmittedPayload{InputID: "a", Content: jsonstable.MustParse(`{"text":"hi"}`), Placement: PlacementTurn, SubmittedAtUnixMilli: 1}},
		{TypeInputSubmitted, InputSubmittedPayload{InputID: "w", Content: jsonstable.MustParse(`{"text":"hi"}`), Placement: PlacementNextTurn, SubmittedAtUnixMilli: 1}},
		{TypeInputWithdrawn, InputWithdrawnPayload{InputID: "w", Reason: "withdrawn"}},
		{TypeInputSubmitted, InputSubmittedPayload{InputID: "r", Content: jsonstable.MustParse(`{"text":"hi"}`), Placement: PlacementNextTurn, SubmittedAtUnixMilli: 1}},
		{TypeInputRejected, InputRejectedPayload{InputID: "r", Reason: "rejected"}},
		accepted("t", "z"),
		completed("t", "s", "sha256:result"), // unrelated history must not grow Pending
		accepted("t", "a"),
	}
	var commits []ledger.Commit
	for i, st := range steps {
		wire, err := r.Encode(st.typ, st.value)
		if err != nil {
			t.Fatal(err)
		}
		domain := Stream
		if ev, ok := st.value.(sessionstore.Event); ok {
			domain = sessionstore.Stream(ev.TurnID)
		}
		// Several lifecycle facts share a commit, so submission order must use
		// Position.Index as well as Commit, independently of input IDs.
		commit := ledger.Commit{Seq: ledger.CommitSeq(i / 3), Batches: []ledger.EventBatch{{Domain: domain, Events: []ledger.Event{{Type: st.typ, Payload: wire}}}}}
		if i%3 == 0 {
			commits = append(commits, commit)
		} else {
			commits[len(commits)-1].Batches = append(commits[len(commits)-1].Batches, commit.Batches...)
		}
	}
	for _, commit := range commits {
		before := pending.(Pending)
		beforeWire, err := PendingProjection.StateCodec.Encode(before)
		if err != nil {
			t.Fatal(err)
		}
		pending, err = r.Fold(scope, pending, []ledger.Commit{commit})
		if err != nil {
			t.Fatal(err)
		}
		afterWire, err := PendingProjection.StateCodec.Encode(before)
		if err != nil || beforeWire.String() != afterWire.String() {
			t.Fatalf("fold mutated prior state: %v", err)
		}
		surface, err = r.Fold(surfaceScope, surface, []ledger.Commit{commit})
		if err != nil {
			t.Fatal(err)
		}
		assertPendingMatchesSurface(t, pending.(Pending), surface.(Surface))
		wire, err := PendingProjection.StateCodec.Encode(pending)
		if err != nil {
			t.Fatal(err)
		}
		pending, err = PendingProjection.StateCodec.Decode(wire)
		if err != nil {
			t.Fatal(err)
		}
		assertPendingMatchesSurface(t, pending.(Pending), surface.(Surface))
	}
	if got := pending.(Pending).Inputs.Len(); got != 0 {
		t.Fatalf("settled inputs retained: %d", got)
	}
	// Fork just after submissions: queued input retains ledger order and
	// placement, including inputs subsequently settled only on the parent.
	prefix, _ := PendingProjection.Initial()
	prefix, err = r.FoldFrom(scope, prefix, commits[:1], ledger.Head{Next: 1})
	if err != nil {
		t.Fatal(err)
	}
	queued := prefix.(Pending).SubmittedInputs()
	if len(queued) != 3 || queued[0].ID != "z" || queued[1].ID != "a" || queued[1].Placement != PlacementTurn {
		t.Fatalf("inherited queue=%+v", queued)
	}
	// A fork must inherit submission and acceptance with the same domain
	// policies as Surface; accepted parent input must not become pending again.
	inherited, _ := PendingProjection.Initial()
	inherited, err = r.FoldFrom(scope, inherited, commits, ledger.Head{Next: 3})
	if err != nil || inherited.(Pending).Inputs.Len() != 0 {
		t.Fatalf("inherited pending=%+v error=%v", inherited, err)
	}
}

func TestPendingLegacySubmissionAndInvalidTransitions(t *testing.T) {
	r := registry(t)
	event, err := r.Decode(ledger.Event{Type: TypeInputSubmitted, Payload: jsonstable.MustParse(`{"content":{"text":"hi"},"inputId":"old","submittedAtUnixMilli":1,"v":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	initial, _ := PendingProjection.Initial()
	state, err := PendingProjection.Apply(initial, event)
	if err != nil {
		t.Fatal(err)
	}
	pending := state.(Pending)
	if got := pending.SubmittedInputs()[0].Placement; got != PlacementNextTurn {
		t.Fatalf("legacy placement=%q", got)
	}
	for _, value := range []any{
		event.Value,
		InputWithdrawnPayload{InputID: "missing"},
		InputRejectedPayload{InputID: "missing"},
		sessionstore.Event{TurnID: "t", Fact: turn.InputAccepted{Input: turn.AgentInput{ID: "old", Digest: "sha256:wrong"}}},
		sessionstore.Event{TurnID: "t", Fact: turn.TurnEnded{}},
		"unexpected",
	} {
		if _, err := PendingProjection.Apply(pending, module.DecodedEvent{Value: value}); err == nil {
			t.Fatalf("accepted invalid transition %T: %+v", value, value)
		}
	}
	if _, err := PendingProjection.Apply("bad state", event); err == nil {
		t.Fatal("accepted wrong state type")
	}
	wire, err := PendingProjection.StateCodec.Encode(pending)
	if err != nil {
		t.Fatal(err)
	}
	copyState, err := PendingProjection.StateCodec.Decode(wire)
	if err != nil {
		t.Fatal(err)
	}
	copyPending := copyState.(Pending)
	copyPending.Inputs = copyPending.Inputs.Delete("old")
	ordered := pending.SubmittedInputs()
	ordered[0].ID = "mutated"
	if !pending.Inputs.Has("old") || pending.SubmittedInputs()[0].ID != "old" {
		t.Fatal("caller mutation escaped")
	}
}

func TestPendingRetentionDependsOnQueue(t *testing.T) {
	state, _ := PendingProjection.Initial()
	content := jsonstable.MustParse(`{"text":"hi"}`)
	for i := 0; i < 2000; i++ {
		id := InputID(fmt.Sprint(i))
		var err error
		state, err = PendingProjection.Apply(state, module.DecodedEvent{Value: InputSubmittedPayload{InputID: id, Content: content, Placement: PlacementNextTurn}})
		if err != nil {
			t.Fatal(err)
		}
		state, err = PendingProjection.Apply(state, module.DecodedEvent{Value: InputWithdrawnPayload{InputID: id}})
		if err != nil {
			t.Fatal(err)
		}
	}
	wire, err := PendingProjection.StateCodec.Encode(state)
	if err != nil || wire.String() != `{"inputs":{}}` {
		t.Fatalf("retained historical state: %s, %v", wire.String(), err)
	}
}

type pendingTestReader struct {
	state any
	err   error
}

func (r pendingTestReader) Load(_ context.Context, _ session.SessionID, id module.ProjectionID, version module.ProjectionVersion) (any, ledger.Head, error) {
	if id != PendingProjectionID || version != PendingProjection.Version {
		panic("wrong projection")
	}
	return r.state, ledger.Head{}, r.err
}
func TestReadPendingPropagatesFailures(t *testing.T) {
	failure := errors.New("read failed")
	if _, err := ReadPending(context.Background(), pendingTestReader{err: failure}, "s"); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if _, err := ReadPending(context.Background(), pendingTestReader{state: Surface{}}, "s"); err == nil {
		t.Fatal("wrong state accepted")
	}
	want := Pending{}
	got, err := ReadPending(context.Background(), pendingTestReader{state: want}, "s")
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%+v error=%v", got, err)
	}
}

func BenchmarkInputRoutingReadModel(b *testing.B) {
	for _, history := range []int{40, 120, 1000} {
		surface := Surface{}
		// Settled assistant history is irrelevant to routing, but Surface
		// readers otherwise decode it on every scheduling/driver step.
		for _, event := range benchEvents(history) {
			state, err := SurfaceProjection.Apply(surface, event)
			if err != nil {
				b.Fatal(err)
			}
			surface = state.(Surface)
		}
		for _, tc := range []struct {
			name  string
			def   module.ProjectionDefinition
			state any
		}{
			{"surface", SurfaceProjection, surface},
			{"pending", PendingProjection, Pending{}},
		} {
			b.Run(fmt.Sprintf("%d/%s", history, tc.name), func(b *testing.B) {
				wire, err := tc.def.StateCodec.Encode(tc.state)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := tc.def.StateCodec.Decode(wire); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
