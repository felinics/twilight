package filestore

import (
	"context"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/session"
	"os"
	"path/filepath"
	"testing"
)

// DeleteRecord tombstones the root and clears its path without reclaiming.
// Collect then truncates a segment another live path still names, and
// removes a segment no path names. The path on the root is the only
// reference; a segment directory does not carry a second copy.
func TestCollectReclaimsAfterDeleteRecord(t *testing.T) {
	ctx := context.Background()
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	header, err := store.Create(ctx, session.CreateRequest{SessionID: "p", CreatedAtUnixMilli: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(store.segmentDir(header.ID), "covers.json")); !os.IsNotExist(err) {
		t.Fatalf("covers.json present: %v", err)
	}
	w, err := store.Open(ctx, "p", session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	appendOne := func(id string) {
		t.Helper()
		if _, err := w.Append(ctx, ledger.Proposal{CommitID: ledger.CommitID(id), Batches: []ledger.EventBatch{{
			Domain: ledger.Domain{Name: "chat"},
			Events: []ledger.Event{{Type: "twilight/x/n", Payload: jsonstable.MustParse(`{"n":1}`)}},
		}}}); err != nil {
			t.Fatal(err)
		}
	}
	appendOne("c0")
	appendOne("c1")
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: "c", CreatedAtUnixMilli: 2, Fork: &session.ForkOrigin{Session: "p", Seq: 0}}); err != nil {
		t.Fatal(err)
	}
	only, err := store.Create(ctx, session.CreateRequest{SessionID: "only", CreatedAtUnixMilli: 3})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := store.DeleteRecord(ctx, "p")
	if err != nil || len(rec.Path) != 1 || rec.Path[0].Segment != header.ID || !rec.Path[0].End.Open {
		t.Fatalf("deleted record = %+v %v", rec, err)
	}
	if _, err := store.DeleteRecord(ctx, "only"); err != nil {
		t.Fatal(err)
	}
	commits, _, _, err := store.ReadSegment(ctx, header.ID, 0, 0)
	if err != nil || len(commits) != 2 {
		t.Fatalf("segment before collect = %d commits, %v", len(commits), err)
	}
	report, err := store.Collect(ctx)
	if err != nil || report.Truncated[header.ID] != 1 || len(report.Dropped[header.ID]) != 1 || report.Dropped[header.ID][0] != "c1" {
		t.Fatalf("collect repair = %+v %v", report, err)
	}
	removed := map[session.SegmentID]bool{}
	for _, id := range report.Removed {
		removed[id] = true
	}
	if len(removed) != 1 || !removed[only.ID] {
		t.Fatalf("collect removed = %+v, want only's segment", report.Removed)
	}
	commits, _, _, err = store.ReadSegment(ctx, header.ID, 0, 0)
	if err != nil || len(commits) != 1 || commits[0].CommitID != "c0" {
		t.Fatalf("segment after collect = %+v %v", commits, err)
	}
	page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "c"})
	if err != nil || len(page.Commits) != 1 || page.Commits[0].CommitID != "c0" {
		t.Fatalf("child after collect = %+v %v", page.Commits, err)
	}
}
