package filestore

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/session"
)

// TestReadIndexedMatchesFullParse checks the byte-offset read path against a
// Store instance that has no index and parses the whole log: every From and
// Limit must give the same page, and the index must fall back to a full parse
// once another instance changes the file.
func TestReadIndexedMatchesFullParse(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	const sid session.SessionID = "idx"
	indexed, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	header, err := indexed.Create(ctx, session.CreateRequest{SessionID: sid, CreatedAtUnixMilli: 1})
	if err != nil {
		t.Fatal(err)
	}
	seg := header.ID
	w, err := indexed.Open(ctx, sid, session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	appendCommits(t, w, 0, [][]string{{"x", "y"}, {"x"}, {"x", "x", "y"}}) // commits 0..2
	if indexed.index[seg] == nil {
		t.Fatal("append did not keep the index current")
	}
	fresh, err := New(root) // never opened: every read is a full parse
	if err != nil {
		t.Fatal(err)
	}
	for from := session.CommitSeq(0); from <= 4; from++ {
		for _, limit := range []uint32{0, 1, 2} {
			req := session.CommitReadRequest{SessionID: sid, From: from, Limit: limit}
			samePage(t, fmt.Sprintf("from=%d limit=%d", from, limit), indexed, fresh, req)
		}
	}
	// The largest CommitSeq is past the head on both paths; an int conversion
	// of it would wrap negative.
	samePage(t, "from=max", indexed, fresh, session.CommitReadRequest{SessionID: sid, From: math.MaxUint64})
	if indexed.index[seg] == nil {
		t.Fatal("reads dropped the index")
	}

	// Another instance takes over and appends: the file changed under the
	// index, so the next read must rebuild rather than trust it.
	w2, err := fresh.Open(ctx, sid, session.OpenOptions{Takeover: true})
	if err != nil {
		t.Fatal(err)
	}
	appendCommits(t, w2, 3, [][]string{{"y", "y"}}) // commit 3
	page, err := indexed.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid})
	if err != nil || page.Head.Next != 4 || len(page.Commits) != 4 {
		t.Fatalf("stale index survived a foreign append: head=%+v commits=%d err=%v", page.Head, len(page.Commits), err)
	}
	for from := session.CommitSeq(0); from <= 5; from++ {
		samePage(t, fmt.Sprintf("after foreign append from=%d", from), indexed, fresh, session.CommitReadRequest{SessionID: sid, From: from})
	}

	// A truncated tail on disk is excluded by both paths alike.
	if err := indexed.CrashTail(sid, 3); err != nil {
		t.Fatal(err)
	}
	for from := session.CommitSeq(0); from <= 4; from++ {
		samePage(t, fmt.Sprintf("crashed tail from=%d", from), indexed, fresh, session.CommitReadRequest{SessionID: sid, From: from})
	}
	page, err = indexed.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid})
	if err != nil || page.Head.Next != 3 {
		t.Fatalf("dropped commit exposed: head=%+v err=%v", page.Head, err)
	}
}

func appendCommits(t *testing.T, w session.Handle, firstCommit int, groups [][]string) {
	t.Helper()
	for i, g := range groups {
		batch := session.StreamBatch{Stream: session.StreamRef{Domain: "chat"}}
		for j, kind := range g {
			batch.Events = append(batch.Events, session.Event{Type: session.EventType("twilight/" + kind + "/e"), RecordedAtUnixMilli: 1,
				Payload: jsonstable.MustParse(fmt.Sprintf(`{"g":%d,"i":%d}`, firstCommit+i, j))})
		}
		if _, err := w.Append(context.Background(), session.Proposal{CommitID: session.CommitID(fmt.Sprintf("c%d", firstCommit+i)), Batches: []session.StreamBatch{batch}}); err != nil {
			t.Fatalf("append c%d: %v", firstCommit+i, err)
		}
	}
}

// samePage compares indexed against a Store instance created for this one
// call: it has never opened or read the Session, so its ReadCommits is a full
// parse.
func samePage(t *testing.T, name string, indexed, _ *Store, req session.CommitReadRequest) {
	t.Helper()
	fresh, err := New(indexed.root)
	if err != nil {
		t.Fatal(err)
	}
	a, errA := indexed.ReadCommits(context.Background(), req)
	b, errB := fresh.ReadCommits(context.Background(), req)
	if (errA != nil) != (errB != nil) {
		t.Fatalf("%s: errors differ: indexed=%v fresh=%v", name, errA, errB)
	}
	if errA != nil {
		return
	}
	if a.Head != b.Head || a.HasMore != b.HasMore || len(a.Commits) != len(b.Commits) {
		t.Fatalf("%s: pages differ: indexed head=%+v more=%v commits=%d; fresh head=%+v more=%v commits=%d",
			name, a.Head, a.HasMore, len(a.Commits), b.Head, b.HasMore, len(b.Commits))
	}
	for i := range a.Commits {
		if a.Commits[i].Seq != b.Commits[i].Seq || a.Commits[i].CommitID != b.Commits[i].CommitID {
			t.Fatalf("%s: commit %d differs: %+v vs %+v", name, i, a.Commits[i], b.Commits[i])
		}
	}
}
