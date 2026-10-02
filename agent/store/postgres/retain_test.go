package postgres_test

import (
	"context"
	"github.com/felinics/twilight/agent/store/postgres/postgrestest"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/session"
	"testing"
)

// TruncateSegment re-reads the live span under the segment lock, so a cut
// requested below that span does not remove the commits it covers.
// CreateSession takes the same lock and refuses a path whose commit is
// already gone, leaving no root.
func TestTruncateObeysLiveSpan(t *testing.T) {
	ctx := context.Background()
	store := postgrestest.Open(t).Sessions()
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

	if head, dropped, err := store.TruncateSegment(ctx, header.ID, c0.Seq); err != nil || len(dropped) != 0 || head.Next != c2.Seq+1 {
		t.Fatalf("truncate under open span = head %v dropped %v err %v", head, dropped, err)
	}
	if _, err := store.DeleteRecord(ctx, "p"); err != nil {
		t.Fatal(err)
	}
	head, dropped, err := store.TruncateSegment(ctx, header.ID, c0.Seq)
	if err != nil || head.Next != c1.Seq+1 || len(dropped) != 1 || dropped[0] != c2.CommitID {
		t.Fatalf("truncate to stale through = head %v dropped %v err %v", head, dropped, err)
	}
	idx, idxHead, err := store.IndexOf(ctx, header.ID)
	if err != nil || idxHead.Next != c1.Seq+1 || len(idx.Entries) != 2 || idx.Entries[0].CommitID != c0.CommitID || idx.Entries[1].CommitID != c1.CommitID {
		t.Fatalf("segment after stale truncate = head %v entries %+v err %v", idxHead, idx.Entries, err)
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
	if !session.IsNotFound(err) {
		t.Fatalf("create after truncate = %v, want not found", err)
	}
	if _, err := store.Record(ctx, "ghost"); !session.IsNotFound(err) {
		t.Fatalf("ghost root = %v", err)
	}
	if _, _, err := store.IndexOf(ctx, "child"); !session.IsNotFound(err) {
		t.Fatalf("child segment = %v", err)
	}
	page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "high"})
	if err != nil || len(page.Commits) != 2 || page.Commits[1].CommitID != c1.CommitID {
		t.Fatalf("high after truncate = %+v %v", page.Commits, err)
	}
}
