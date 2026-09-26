package writer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
)

// EXT-WRT-11: a Writer opened with a lease keeps it renewed for as long as
// it is open, and learns of a takeover through the heartbeat, after which
// it refuses to commit like a Writer fenced at Append.
func TestWriterHeartbeatRenewsAndReportsLoss(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	opts := session.OpenOptions{Owner: "a", LeaseDuration: 60 * time.Millisecond}
	w, err := OpenWriter(ctx, f.store, f.registry, f.admission(), "s", opts)
	if err != nil {
		t.Fatal(err)
	}
	// Well past the lease duration the Session is still owned: the
	// heartbeat renewed it.
	time.Sleep(150 * time.Millisecond)
	if _, err := f.store.Open(ctx, "s", session.OpenOptions{Owner: "b", LeaseDuration: time.Second}); !session.IsCode(err, session.ErrOwned) {
		t.Fatalf("open while heartbeat renews = %v, want owned", err)
	}
	if res, err := w.Commit(ctx, noteGroup("c1", "one")); err != nil || res.Outcome != CommitApplied {
		t.Fatalf("commit under a renewed lease = %+v %v", res, err)
	}
	// A takeover fences the heartbeat's next Renew; the Writer is lost.
	taker, err := f.store.Open(ctx, "s", session.OpenOptions{Takeover: true, Owner: "b"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = taker.Close(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err := w.Commit(ctx, noteGroup("c2", "two"))
		if errors.Is(err, &extension.Error{Code: extension.ErrOwnershipLost}) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("writer not marked lost after takeover: commit = %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatalf("close of a lost writer = %v", err)
	}
	page, err := f.store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s"})
	if err != nil || len(page.Commits) != 1 {
		t.Fatalf("ledger after takeover = %d commits %v, want only c1", len(page.Commits), err)
	}
}
