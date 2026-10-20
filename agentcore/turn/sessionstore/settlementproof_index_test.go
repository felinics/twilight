package sessionstore

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/preset/ref"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/schema"
	"github.com/felinics/twilight/agentcore/turn/store"
)

type indexedProofScope struct {
	proofScope
	handle session.Handle
}

func (w indexedProofScope) Header() session.SegmentHeader { return w.handle.Header() }
func (w indexedProofScope) LookupCommit(id ledger.CommitID) (ledger.Commit, bool, error) {
	return w.handle.LookupCommit(id)
}

type proofReadCounter struct {
	session.Store
	streams, commits, events int
	from                     ledger.CommitSeq
}

func (s *proofReadCounter) ReadStream(ctx context.Context, req session.StreamReadRequest) (session.StreamPage, error) {
	s.streams++
	page, err := s.Store.ReadStream(ctx, req)
	s.events += len(page.Events)
	return page, err
}
func (s *proofReadCounter) ReadCommits(ctx context.Context, req session.CommitReadRequest) (session.CommitPage, error) {
	s.commits++
	s.from = req.From
	page, err := s.Store.ReadCommits(ctx, req)
	s.events += len(page.Commits)
	return page, err
}

func TestIndexedEffectProofBoundsLongTurnReads(t *testing.T) {
	for _, path := range []string{"settlement", "cancel", "pending", "foreign_settlement", "legacy_cancel", "cancel_paged"} {
		t.Run(path, func(t *testing.T) {
			ctx := context.Background()
			durable := filestoretest.Store(t)
			reg, err := module.BuildRegistry(Module)
			if err != nil {
				t.Fatal(err)
			}
			const sid session.SessionID = "proof"
			const run turn.TurnID = "run"
			const effect turn.EffectID = "effect"
			if _, err := durable.Create(ctx, session.CreateRequest{SessionID: sid}); err != nil {
				t.Fatal(err)
			}
			h, err := durable.Open(ctx, sid, session.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close(ctx)
			appendProofFacts(t, h, reg, run, "create", turn.TurnStarted{Preset: ref.Ref{ID: "test", Digest: "sha256:test"}, TurnID: run})
			for i := 0; i < 140; i++ {
				appendProofFacts(t, h, reg, run, ledger.CommitID(fmt.Sprintf("input-%d", i)), turn.InputAccepted{Input: turn.AgentInput{ID: turn.InputID(fmt.Sprintf("input-%d", i)), Digest: "sha256:input"}})
			}
			startID := ledger.CommitID(schema.Identity().DeriveStartCommandID(effect))
			if path == "legacy_cancel" {
				startID = "legacy-start"
			}
			start := appendProofFacts(t, h, reg, run, startID, turn.ModelStepPrepared{StepID: "step", Model: "model", RequestDigest: "sha256:request"}, turn.ModelStepStarted{StepID: "step", Effect: effect})
			if path == "cancel_paged" {
				for i := 0; i < 130; i++ {
					appendProofFacts(t, h, reg, run, ledger.CommitID(fmt.Sprintf("after-start-%d", i)), turn.InputAccepted{Input: turn.AgentInput{ID: turn.InputID(fmt.Sprintf("after-start-%d", i)), Digest: "sha256:input"}})
				}
			}
			state := turn.MachineState{TurnID: run, Status: turn.TurnActive, Current: turn.ModelStep{RefValue: turn.StepRef{TurnID: run, ID: "step"}, Status: turn.ModelExecuting, Effect: effect}}
			closing, err := schema.Machine().Decide(state, turn.CancelTurn{})
			if err != nil {
				t.Fatal(err)
			}
			switch path {
			case "settlement":
				appendProofFacts(t, h, reg, run, ledger.CommitID(schema.Identity().DeriveSettlementCommandID(effect)), closing...)
			case "cancel", "legacy_cancel", "cancel_paged":
				appendProofFacts(t, h, reg, run, "arbitrary-cancel-id", closing...)
			case "foreign_settlement":
				appendProofFacts(t, h, reg, "foreign-turn", ledger.CommitID(schema.Identity().DeriveSettlementCommandID(effect)), closing...)
			}
			counted := &proofReadCounter{Store: durable}
			adapter := &SessionTurnStore{cfg: Config{Store: counted, Registry: reg}}
			proof := adapter.Bind(indexedProofScope{proofScope: proofScope{sid: sid}, handle: h}).(*bound)
			facts, found, err := proof.EffectDisposition(ctx, run, effect)
			want := path != "pending" && path != "foreign_settlement"
			if err != nil || found != want {
				t.Fatalf("proof=%+v found=%v err=%v", facts, found, err)
			}
			if found && (len(facts) != 1 || store.DispositionEffect(facts[0]) != effect) {
				t.Fatalf("facts=%+v", facts)
			}
			checkProofReadBounds(t, path, counted, start.Seq)
			// A descendant must never authorize collection using an ancestor's proof.
			if path == "settlement" {
				if _, err := durable.Create(ctx, session.CreateRequest{SessionID: "child", Fork: &session.ForkOrigin{Session: sid, Seq: start.Seq + 1}}); err != nil {
					t.Fatal(err)
				}
				child, err := durable.Open(ctx, "child", session.OpenOptions{})
				if err != nil {
					t.Fatal(err)
				}
				defer child.Close(ctx)
				childProof := adapter.Bind(indexedProofScope{proofScope: proofScope{sid: "child"}, handle: child}).(*bound)
				if facts, found, err := childProof.EffectDisposition(ctx, run, effect); err != nil || found || len(facts) > 0 {
					t.Fatalf("inherited proof accepted: %+v %v %v", facts, found, err)
				}
			}
		})
	}
}

func checkProofReadBounds(t *testing.T, path string, counted *proofReadCounter, start ledger.CommitSeq) {
	t.Helper()
	if path == "legacy_cancel" {
		if counted.streams != 2 || counted.commits != 0 {
			t.Fatalf("legacy proof did not use full stream fallback: %+v", counted)
		}
	} else if path == "cancel_paged" {
		if counted.streams != 0 || counted.commits != 2 || counted.events != 132 || counted.from != start+128 {
			t.Fatalf("bounded proof did not advance across pages: %+v", counted)
		}
	} else if counted.streams != 0 || counted.events > 2 {
		t.Fatalf("replayed history: %+v", counted)
	}
	if path == "settlement" && counted.commits != 0 {
		t.Fatalf("indexed settlement read tail: %+v", counted)
	}
	if path != "settlement" && path != "legacy_cancel" && path != "cancel_paged" && (counted.commits != 1 || counted.from != start) {
		t.Fatalf("tail did not start at verified start: %+v", counted)
	}
}

type failingIndexedProof struct {
	proofScope
	err error
}

func (w failingIndexedProof) LookupCommit(ledger.CommitID) (ledger.Commit, bool, error) {
	return ledger.Commit{}, false, w.err
}
func TestIndexedProofFailureDoesNotAuthorizeCollection(t *testing.T) {
	boom := errors.New("commit index unavailable")
	b := &bound{w: failingIndexedProof{proofScope: proofScope{sid: "session"}, err: boom}}
	if _, found, err := b.EffectDisposition(context.Background(), "turn", "effect"); found || !errors.Is(err, boom) {
		t.Fatalf("found=%v err=%v", found, err)
	}
}
