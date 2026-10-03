package filestore

import (
	"context"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/session"
	"os"
	"testing"
)

// A truncation asked to cut below a live span keeps that span's commits.
// Creating a path for a commit the truncation already removed fails and
// leaves no root. The store lock makes the span check and the cut one
// critical section, which is the file-store form of the cross-process rule.
func TestTruncateObeysLiveSpan(t *testing.T) {
	ctx := context.Background()
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	header, err := store.Create(ctx, session.CreateRequest{SessionID: "p", CreatedAtUnixMilli: 1})
	if err != nil {
		t.Fatal(err)
	}
	w, err := store.Open(ctx, "p", session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	appendOne := func(id string) ledger.Commit {
		t.Helper()
		c, err := w.Append(ctx, ledger.Proposal{CommitID: ledger.CommitID(id), Batches: []ledger.EventBatch{{
			Domain: ledger.Domain{Name: "chat"},
			Events: []ledger.Event{{Type: "twilight/x/n", Payload: jsonstable.MustParse(`{"n":1}`)}},
		}}})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	c0 := appendOne("c0")
	c1 := appendOne("c1")
	c2 := appendOne("c2")
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(ctx, session.CreateRequest{
		SessionID: "high", CreatedAtUnixMilli: 2,
		Fork: &session.ForkOrigin{Session: "p", Seq: c1.Seq},
	}); err != nil {
		t.Fatal(err)
	}

	// p's span is still open, so a cut at 0 must not drop c1 or c2.
	if head, dropped, err := store.TruncateSegment(ctx, header.ID, c0.Seq); err != nil || len(dropped) != 0 || head.Next != c2.Seq+1 {
		t.Fatalf("truncate under open span = head %v dropped %v err %v", head, dropped, err)
	}

	if _, err := store.DeleteRecord(ctx, "p"); err != nil {
		t.Fatal(err)
	}
	// high still covers c1. A caller that only saw c0 must not drop c1.
	head, dropped, err := store.TruncateSegment(ctx, header.ID, c0.Seq)
	if err != nil || head.Next != c1.Seq+1 || len(dropped) != 1 || dropped[0] != c2.CommitID {
		t.Fatalf("truncate to stale through = head %v dropped %v err %v", head, dropped, err)
	}
	commits, _, _, err := store.ReadSegment(ctx, header.ID, 0, 0)
	if err != nil || len(commits) != 2 || commits[0].CommitID != c0.CommitID || commits[1].CommitID != c1.CommitID {
		t.Fatalf("segment after stale truncate = %+v %v", commits, err)
	}

	err = store.CreateSession(ctx, session.Segment{Header: session.SegmentHeader{
		ID:     "child",
		Parent: &session.CommitRef{Segment: header.ID, Seq: c2.Seq},
	}}, session.SessionRecord{
		ID: "ghost", Tip: "child", CreatedAtUnixMilli: 3,
		Path: session.Path{
			{Segment: header.ID, From: 0, End: session.ThroughBound(c2.Seq)},
			{Segment: "child", From: c2.Seq + 1, End: session.OpenBound()},
		},
	})
	if !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("create after truncate = %v, want not found", err)
	}
	if _, err := os.Stat(store.segmentDir("child")); !os.IsNotExist(err) {
		t.Fatalf("child segment left behind: %v", err)
	}
	if _, err := store.Record(ctx, "ghost"); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("ghost root = %v", err)
	}
	page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "high"})
	if err != nil || len(page.Commits) != 2 || page.Commits[1].CommitID != c1.CommitID {
		t.Fatalf("high after truncate = %+v %v", page.Commits, err)
	}
}
