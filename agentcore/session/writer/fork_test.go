package writer

import (
	"context"
	"testing"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
)

// SES-FRK-2/3, EXT-WRT-8: a Writer over a fork folds its projections from
// the inherited prefix, answers a replay of an inherited CommitID as already
// applied and continues the ledger from the anchor; Fork itself holds no
// claim, the prefix's content stays under the parent segment's commit
// claims.
func TestForkWriterInheritsPrefix(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	ref := artifact.Ref{Scheme: "cas", Authority: "local", Key: "k1", Durability: artifact.EventBound, Integrity: &artifact.Integrity{Algorithm: "sha256", Value: "x"}}
	binding, err := artifact.NewBinding("b1", ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.bindings.CreateBinding(ctx, binding); err != nil {
		t.Fatal(err)
	}
	parent := f.open(t, false)
	if _, err := parent.Commit(ctx, noteGroup("c1", "one")); err != nil {
		t.Fatal(err)
	}
	withRef := func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c2", Batches: noteBatch(TypedEvent{Type: tpfx("a") + "note", Value: notePayload{Text: "two", Refs: []string{"b1"}}})}, nil
	}
	if res, err := parent.Commit(ctx, withRef); err != nil || res.Outcome != CommitApplied {
		t.Fatalf("c2 = %+v %v", res, err)
	}
	if _, err := parent.Commit(ctx, noteGroup("c3", "three")); err != nil {
		t.Fatal(err)
	}
	parentSeg := tipSegment(t, f.store, "s")

	header, err := Fork(ctx, f.store, f.registry, ForkRequest{Parent: "s", At: 1, Child: "child", CreatedAtUnixMilli: 5})
	if err != nil {
		t.Fatal(err)
	}
	if header.Parent == nil || header.Parent.Seq != 1 {
		t.Fatalf("header = %+v", header)
	}
	if _, err := Fork(ctx, f.store, f.registry, ForkRequest{Parent: "s", At: 1, Child: "child", CreatedAtUnixMilli: 5}); err != nil {
		t.Fatalf("repeat fork: %v", err)
	}
	// The only claim over b1 is the parent segment's commit claim.
	claims, err := artifact.ActiveClaims(ctx, f.ledger, artifact.ClaimOwnerScope{Kind: ClaimOwnerKind, Authority: string(parentSeg)})
	if err != nil || len(claims) != 1 || claims[0].Owner.Identity != "c2" {
		t.Fatalf("claims after fork = %+v %v, want the parent's c2 claim only", claims, err)
	}

	child, err := OpenWriter(ctx, f.store, f.registry, f.admission(), "child", session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	load := func(w Writer, sid session.SessionID) []string {
		state, _, err := w.Projections().Load(ctx, sid, extension.ProjectionID(string(tpfx("a"))+"notes"), 1)
		if err != nil {
			t.Fatal(err)
		}
		return state.(noteState).Notes
	}
	if got := load(child, "child"); len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("child projection = %v, want the prefix [one two]", got)
	}
	// Opening the child reconciles the child tip's scope only: the parent
	// segment's claim is untouched.
	if claims, _ = artifact.ActiveClaims(ctx, f.ledger, artifact.ClaimOwnerScope{Kind: ClaimOwnerKind, Authority: string(parentSeg)}); len(claims) != 1 {
		t.Fatal("opening the fork released the parent segment's claim")
	}
	replay, err := child.Commit(ctx, noteGroup("c1", "one"))
	if err != nil || replay.Outcome != CommitAlreadyApplied || replay.Commit.Seq != 0 {
		t.Fatalf("replay of inherited c1 = %+v %v", replay, err)
	}
	if again, _ := child.Commit(ctx, noteGroup("c1", "other")); again.Outcome != CommitAlreadyApplied {
		t.Fatalf("replay of inherited c1 with other content = %+v, want already applied", again)
	}
	res, err := child.Commit(ctx, noteGroup("c4", "four"))
	if err != nil || res.Outcome != CommitApplied || res.Commit.Seq != 2 {
		t.Fatalf("own commit = %+v %v", res, err)
	}
	if got := load(child, "child"); len(got) != 3 || got[2] != "four" {
		t.Fatalf("child projection after own commit = %v", got)
	}
	if got := load(parent, "s"); len(got) != 3 || got[2] != "three" {
		t.Fatalf("parent projection = %v, want [one two three]", got)
	}
	state, head, err := extension.NewProjectionReader(f.store, f.registry, nil).Load(ctx, "child", extension.ProjectionID(string(tpfx("a"))+"notes"), 1)
	if err != nil || len(state.(noteState).Notes) != 3 || head.Next != 3 {
		t.Fatalf("observer = %+v %+v %v", state, head, err)
	}
	// A Session cannot fork itself; nothing is created.
	if _, err := Fork(ctx, f.store, f.registry, ForkRequest{Parent: "s", At: 0, Child: "s"}); !session.IsCode(err, session.ErrInvalid) {
		t.Fatalf("self fork = %v, want ErrInvalid", err)
	}
}

// SES-GC-1/2/3, EXT-WRT-9: Delete drops the root and leaves claims alone; a
// fork keeps reading the inherited commits, whose claims stay Active with
// their segment; Collect releases a claim exactly when it reclaims the
// commit, and a repeated Delete is a no-op.
func TestClaimsFollowSegmentsThroughCollect(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	ref := artifact.Ref{Scheme: "cas", Authority: "local", Key: "k1", Durability: artifact.EventBound, Integrity: &artifact.Integrity{Algorithm: "sha256", Value: "x"}}
	binding, _ := artifact.NewBinding("b1", ref)
	if _, err := f.bindings.CreateBinding(ctx, binding); err != nil {
		t.Fatal(err)
	}
	withRef := func(id string) CommitFn {
		return func(View) (*SemanticGroup, error) {
			return &SemanticGroup{CommitID: session.CommitID(id), Batches: noteBatch(TypedEvent{Type: tpfx("a") + "note", Value: notePayload{Text: id, Refs: []string{"b1"}}})}, nil
		}
	}
	parent := f.open(t, false)
	for _, id := range []string{"c1", "c2"} {
		if _, err := parent.Commit(ctx, withRef(id)); err != nil {
			t.Fatal(err)
		}
	}
	parentSeg := tipSegment(t, f.store, "s")
	if _, err := Fork(ctx, f.store, f.registry, ForkRequest{Parent: "s", At: 0, Child: "child"}); err != nil {
		t.Fatal(err)
	}
	if err := parent.Close(ctx); err != nil {
		t.Fatal(err)
	}
	active := func() []string {
		claims, err := artifact.ActiveClaims(ctx, f.ledger, artifact.ClaimOwnerScope{Kind: ClaimOwnerKind, Authority: string(parentSeg)})
		if err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, c := range claims {
			ids = append(ids, c.Owner.Identity)
		}
		return ids
	}
	if err := Delete(ctx, f.store, "s"); err != nil {
		t.Fatal(err)
	}
	if got := active(); len(got) != 2 {
		t.Fatalf("claims after deleting the parent = %v, want both kept (the segment is still reached by the fork)", got)
	}
	if _, err := OpenWriter(ctx, f.store, f.registry, f.admission(), "s", session.OpenOptions{}); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("open deleted = %v", err)
	}
	child, err := OpenWriter(ctx, f.store, f.registry, f.admission(), "child", session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	state, _, err := child.Projections().Load(ctx, "child", extension.ProjectionID(string(tpfx("a"))+"notes"), 1)
	if err != nil || len(state.(noteState).Notes) != 1 || state.(noteState).Notes[0] != "c1" {
		t.Fatalf("child after deleting the parent = %+v %v", state, err)
	}
	// Collect truncates the parent's segment after commit 0: c2 is dropped
	// and its claim released; c1 stays with the retained prefix.
	report, err := Collect(ctx, f.store, f.admission())
	if err != nil || len(report.Removed) != 0 || report.Truncated[parentSeg] != 1 || len(report.Dropped[parentSeg]) != 1 || report.Dropped[parentSeg][0] != "c2" {
		t.Fatalf("collect = %+v %v, want the parent's segment kept through commit 0 with c2 dropped", report, err)
	}
	if got := active(); len(got) != 1 || got[0] != "c1" {
		t.Fatalf("claims after truncation = %v, want c1 only", got)
	}
	if err := child.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Delete(ctx, f.store, "child"); err != nil {
		t.Fatal(err)
	}
	if err := Delete(ctx, f.store, "child"); err != nil {
		t.Fatalf("repeated delete = %v, want nil", err)
	}
	if report, err := Collect(ctx, f.store, f.admission()); err != nil || len(report.Removed) != 2 {
		t.Fatalf("final collect = %+v %v", report, err)
	}
	if got := active(); len(got) != 0 {
		t.Fatalf("claims after the segment is removed = %v, want none", got)
	}
}
