package filestore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/session"
)

func proposal(commit string, n int) session.Proposal {
	batch := session.StreamBatch{Stream: session.StreamRef{Domain: "chat"}}
	for i := 0; i < n; i++ {
		batch.Events = append(batch.Events, session.Event{Type: "twilight/x/e", RecordedAtUnixMilli: 1,
			Payload: jsonstable.MustParse(fmt.Sprintf(`{"c":%q,"i":%d}`, commit, i))})
	}
	return session.Proposal{CommitID: session.CommitID(commit), Batches: []session.StreamBatch{batch}}
}

// SES-APP-1: a commit whose bytes were written but whose fsync failed is
// unknown to the handle. The handle refuses further appends; a reopen finds
// the complete commit on disk, indexes it, and the stream continues after it
// with a valid ledger.
func TestAppendSyncFailurePoisonsHandle(t *testing.T) {
	ctx := context.Background()
	const sid session.SessionID = "sync"
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(ctx, session.CreateRequest{SessionID: sid, CreatedAtUnixMilli: 1}); err != nil {
		t.Fatal(err)
	}
	h, err := s.Open(ctx, sid, session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Append(ctx, proposal("c0", 1)); err != nil {
		t.Fatal(err)
	}

	s.sync = func(*os.File) error { return errors.New("injected fsync failure") }
	if _, err := h.Append(ctx, proposal("c1", 2)); !session.IsCode(err, session.ErrHandleFailed) {
		t.Fatalf("append with failing sync = %v, want handle_failed", err)
	}
	s.sync = nil
	if _, err := h.Append(ctx, proposal("c2", 1)); !session.IsCode(err, session.ErrHandleFailed) {
		t.Fatalf("poisoned handle accepted an append: %v", err)
	}
	if h.Committed("c1") {
		t.Fatal("poisoned handle claims to know a commit whose outcome is unknown")
	}

	// The bytes did land: the reopen sees the complete commit.
	h2, err := s.Open(ctx, sid, session.OpenOptions{Takeover: true})
	if err != nil {
		t.Fatalf("reopen after sync failure: %v", err)
	}
	if !h2.Committed("c1") {
		t.Fatal("reopened handle does not index the commit that reached disk")
	}
	c, ok, err := h2.LookupCommit("c1")
	if err != nil || !ok || c.Seq != 1 || len(c.Batches) != 1 || len(c.Batches[0].Events) != 2 {
		t.Fatalf("lookup c1 = %+v %v %v", c, ok, err)
	}
	if _, err := h2.Append(ctx, proposal("c1", 2)); !session.IsCode(err, session.ErrConflict) {
		t.Fatalf("replaying the durable commit = %v, want conflict", err)
	}
	next, err := h2.Append(ctx, proposal("c2", 1))
	if err != nil || next.Seq != 2 {
		t.Fatalf("append after reopen = %+v %v, want seq 2", next, err)
	}
	page, err := s.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid})
	if err != nil || len(page.Commits) != 3 || page.Head.Next != 3 {
		t.Fatalf("read = %d commits head %+v %v", len(page.Commits), page.Head, err)
	}
}
