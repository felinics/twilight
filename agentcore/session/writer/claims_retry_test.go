package writer

import (
	"context"
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/session"
)

func claimFixture(t *testing.T) (*fixture, artifact.BindingSet, CommitFn) {
	t.Helper()
	ctx := context.Background()
	f := newFixture(t)
	binding, err := artifact.NewBinding("b1", artifact.Ref{Scheme: "spill", Authority: "local", Key: "k", Durability: artifact.EventBound})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.bindings.CreateBinding(ctx, binding); err != nil {
		t.Fatal(err)
	}
	set, err := (artifact.SetBuilder{Resolver: f.bindings}).Build(ctx, []artifact.BindingID{binding.ID})
	if err != nil {
		t.Fatal(err)
	}
	return f, set, func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c1", Batches: noteBatch(TypedEvent{Type: tpfx("a") + "note", Value: notePayload{Text: "file", Refs: []string{"b1"}}})}, nil
	}
}

func TestWriterRetriesReleasedClaims(t *testing.T) {
	ctx := context.Background()
	f, set, group := claimFixture(t)
	fs := &faultStore{Store: f.store}
	open := func() Writer {
		t.Helper()
		w, err := OpenWriter(ctx, fs, f.registry, f.admission(), "s", session.OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return w
	}
	w := open()
	defer func() { _ = w.Close(ctx) }()
	id := DeriveClaimID(tipSegment(t, f.store, "s"), "c1", set.RefSetDigest)
	for _, mode := range []string{"invalid", "invalid", "before", "before"} {
		fs.arm(mode)
		if _, err := w.Commit(ctx, group); err == nil {
			t.Fatal("faulted append succeeded")
		}
		if mode == "before" {
			if err := w.Close(ctx); err != nil {
				t.Fatal(err)
			}
			w = open()
		}
		if claim, ok, err := f.ledger.LookupClaim(ctx, id); err != nil || !ok || claim.State != artifact.ClaimReleased {
			t.Fatalf("released claim %s = %+v, %v, %v", id, claim, ok, err)
		}
		id = nextClaimID(id)
	}
	res, err := w.Commit(ctx, group)
	if err != nil || res.Outcome != CommitApplied || res.Claim == nil || res.Claim.ID != id || res.Claim.State != artifact.ClaimActive {
		t.Fatalf("eventual commit = %+v, %v; want claim %s", res, err, id)
	}
	res, err = w.Commit(ctx, group)
	if err != nil || res.Outcome != CommitAlreadyApplied {
		t.Fatalf("replay after claim retries = %+v, %v", res, err)
	}
	active, err := artifact.ActiveClaims(ctx, f.ledger, artifact.ClaimOwnerScope{Kind: ClaimOwnerKind, Authority: string(tipSegment(t, f.store, "s"))})
	if err != nil || len(active) != 1 || active[0].ID != id {
		t.Fatalf("retention roots = %+v, %v", active, err)
	}
}

func TestWriterRejectsMismatchedReleasedClaim(t *testing.T) {
	for _, field := range []string{"owner", "digest", "binding_ids"} {
		t.Run(field, func(t *testing.T) {
			ctx := context.Background()
			f, set, group := claimFixture(t)
			// An unverified ledger lets the test seed each possible mismatch.
			f.ledger = &artifacttest.MapLedger{}
			w := f.open(t, false)
			defer w.Close(ctx)
			seg := tipSegment(t, f.store, "s")
			id := DeriveClaimID(seg, "c1", set.RefSetDigest)
			owner := CommitOwner(seg, "c1")
			switch field {
			case "owner":
				owner.Identity = "other"
			case "digest":
				set.RefSetDigest = "other"
			case "binding_ids":
				set.BindingIDs = []artifact.BindingID{"other"}
			}
			if _, err := f.ledger.Activate(ctx, id, owner, set); err != nil {
				t.Fatal(err)
			}
			if err := f.ledger.ReleaseActive(ctx, id); err != nil {
				t.Fatal(err)
			}
			res, err := w.Commit(ctx, group)
			if err != nil || res.Outcome != CommitInvalid || !strings.Contains(res.Detail, "owner or binding set conflicts") {
				t.Fatalf("mismatched claim commit = %+v, %v", res, err)
			}
			if _, ok, err := f.ledger.LookupClaim(ctx, nextClaimID(id)); err != nil || ok {
				t.Fatalf("successor of mismatched claim exists = %v, %v", ok, err)
			}
			if page, err := f.store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s"}); err != nil || len(page.Commits) != 0 {
				t.Fatalf("mismatched claim wrote commits = %+v, %v", page.Commits, err)
			}
		})
	}
}
