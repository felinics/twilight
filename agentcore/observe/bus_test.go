package observe_test

import (
	"context"
	"errors"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/observe"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/session/writer"
	"testing"
	"time"
)

type rowPayload struct {
	Text string `json:"text"`
}

const rowType ledger.EventType = "twilight/z/row"

func registry(t *testing.T) *module.Registry {
	t.Helper()
	r, err := module.BuildRegistry(module.ModuleDescriptor{Source: module.SourceTwilight, ID: "z",
		Streams: []module.StreamDefinition{{Domain: "z", Inheritance: module.Inherited}},
		Events: []module.EventDefinition{{Type: rowType, Domain: "z",
			Codecs: map[module.PayloadVersion]module.PayloadCodec{1: module.JSONCodec[rowPayload]{}}}}})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func commitRow(t *testing.T, w writer.Writer, id, text string) {
	t.Helper()
	res, err := w.Commit(context.Background(), func(writer.View) (*writer.SemanticGroup, error) {
		return &writer.SemanticGroup{CommitID: ledger.CommitID(id), Batches: []writer.TypedBatch{{Domain: ledger.Domain{Name: "z"},
			Events: []writer.TypedEvent{{Type: rowType, Value: rowPayload{Text: text}}}}}}, nil
	})
	if err != nil || res.Outcome != writer.CommitApplied {
		t.Fatalf("commit %s: outcome=%s err=%v", id, res.Outcome, err)
	}
}

func texts(t *testing.T, ch <-chan observe.Event, n int) ([]string, []ledger.Position) {
	t.Helper()
	var got []string
	var at []ledger.Position
	deadline := time.After(5 * time.Second)
	for len(got) < n {
		select {
		case e, ok := <-ch:
			if !ok {
				t.Fatalf("stream closed after %d of %d events", len(got), n)
			}
			if e.Err != nil {
				t.Fatal(e.Err)
			}
			got = append(got, e.Value.(rowPayload).Text)
			at = append(at, e.Position)
		case <-deadline:
			t.Fatalf("got %d of %d events: %v", len(got), n, got)
		}
	}
	return got, at
}

// A catch-up subscription delivers the ledger from the requested position,
// then the live commits, each event once and in order, with the Position a
// consumer checkpoints; a live-only subscription sees only what follows.
func TestSubscribeFromCatchesUpThenGoesLive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := filestoretest.Store(t)
	reg := registry(t)
	bus := observe.NewBus(reg, store)
	const sid session.SessionID = "s1"
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: sid}); err != nil {
		t.Fatal(err)
	}
	w, err := writer.NewWriters(store, reg, writer.Admission{}, session.OpenOptions{}, writer.WritersConfig{Observers: []writer.CommitObserver{bus}}).Writer(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close(ctx)
	commitRow(t, w, "c1", "a")
	commitRow(t, w, "c2", "b")
	commitRow(t, w, "c3", "c")

	// Each case commits one more row after subscribing, so the ledger grows
	// a, b, c | d | e | f and every case sees its history plus its own live
	// commit, nothing of the cases before or after.
	cases := []struct {
		name string
		from ledger.CommitSeq
		live string
		want []string
	}{
		{"from the start", 0, "d", []string{"a", "b", "c", "d"}},
		{"from a checkpoint", 2, "e", []string{"c", "d", "e"}},
		{"from the head", 5, "f", []string{"f"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sctx, scancel := context.WithCancel(ctx)
			defer scancel()
			ch, err := bus.SubscribeFrom(sctx, sid, tc.from)
			if err != nil {
				t.Fatal(err)
			}
			commitRow(t, w, "live-"+tc.live, tc.live)
			got, at := texts(t, ch, len(tc.want))
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("events = %v, want %v", got, tc.want)
				}
			}
			for i := 1; i < len(at); i++ {
				if !at[i-1].Less(at[i]) {
					t.Fatalf("positions not increasing: %v", at)
				}
			}
			if at[0].Commit < tc.from {
				t.Fatalf("first position %v is below from %d", at[0], tc.from)
			}
			// Nothing else is pending: the history and the live commit were
			// delivered exactly once.
			select {
			case e := <-ch:
				t.Fatalf("unexpected extra event %v", e)
			case <-time.After(50 * time.Millisecond):
			}
		})
	}

	live := bus.Subscribe(ctx, sid)
	commitRow(t, w, "g", "g")
	got, _ := texts(t, live, 1)
	if got[0] != "g" {
		t.Fatalf("live = %v, want [g]", got)
	}
	if _, err := observe.NewBus(reg, nil).SubscribeFrom(ctx, sid, 0); !errors.Is(err, observe.ErrNoHistory) {
		t.Fatalf("SubscribeFrom without history = %v, want ErrNoHistory", err)
	}
}
