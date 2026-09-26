package writer

import (
	"context"
	"sync"
	"testing"

	"github.com/felinics/twilight/agentcore/session"
)

// recordingObserver keeps every notification in the order it arrived.
type recordingObserver struct {
	mu      sync.Mutex
	commits []session.Commit
}

func (o *recordingObserver) Committed(_ context.Context, _ session.SessionID, commit session.Commit) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.commits = append(o.commits, commit)
}

type panickingObserver struct{}

func (panickingObserver) Committed(context.Context, session.SessionID, session.Commit) {
	panic("observer failed")
}

// EXT-WRT-7: every applied commit reaches the observers once, in commit order,
// with the sealed commit; rejected and replayed commits notify nothing; a
// panicking observer neither fails the Commit nor starves the next observer.
func TestCommitObserversSeeAppliedCommitsInOrder(t *testing.T) {
	f := newCacheFixture(t)
	rec := &recordingObserver{}
	w := f.open(t, WritersConfig{Observers: []CommitObserver{panickingObserver{}, rec}})

	f.commit(t, w, "c1", "one", "two")
	f.commit(t, w, "c2", "three")
	// Replay: already applied, no notification.
	res, err := w.Commit(context.Background(), func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c1", Batches: noteBatch(
			TypedEvent{Type: tpfx("k") + "row", Value: notePayload{Text: "one"}},
			TypedEvent{Type: tpfx("k") + "row", Value: notePayload{Text: "two"}})}, nil
	})
	if err != nil || res.Outcome != CommitAlreadyApplied {
		t.Fatalf("replay = %v %v", res.Outcome, err)
	}
	// Rejected: unknown event type, no notification.
	res, err = w.Commit(context.Background(), func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c3", Batches: noteBatch(TypedEvent{Type: "twilight/nope/x", Value: notePayload{Text: "x"}})}, nil
	})
	if err != nil || res.Outcome != CommitInvalid {
		t.Fatalf("invalid = %v %v", res.Outcome, err)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.commits) != 2 {
		t.Fatalf("notifications = %d, want 2", len(rec.commits))
	}
	commits := f.commits(t)
	for i, c := range rec.commits {
		if c.Seq != commits[i].Seq || c.CommitID != commits[i].CommitID {
			t.Fatalf("notification %d = %+v, want log commit %+v", i, c, commits[i])
		}
	}
	if len(rec.commits) != len(commits) {
		t.Fatalf("observed %d commits, log has %d", len(rec.commits), len(commits))
	}
}

// Concurrent committers are notified in the order their commits landed: the
// observer sees a strictly increasing Seq sequence.
func TestCommitObserverOrderUnderConcurrency(t *testing.T) {
	f := newCacheFixture(t)
	rec := &recordingObserver{}
	w := f.open(t, WritersConfig{Observers: []CommitObserver{rec}})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			f.commit(t, w, "p"+string(rune('a'+i)), "n")
		}(i)
	}
	wg.Wait()
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.commits) != 16 {
		t.Fatalf("notifications = %d, want 16", len(rec.commits))
	}
	for i := 1; i < len(rec.commits); i++ {
		if rec.commits[i].Seq <= rec.commits[i-1].Seq {
			t.Fatalf("notification %d (seq %d) arrived after seq %d", i, rec.commits[i].Seq, rec.commits[i-1].Seq)
		}
	}
}
