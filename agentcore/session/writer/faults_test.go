package writer

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
)

// faultStore wraps a Store so one Append can be made to fail either before
// the kernel writes (nothing reaches the log) or after it has written (the
// commit is durable but the caller gets an error instead of it). Both are
// "outcome unknown" from the Writer's side; the log tells them apart.
type faultStore struct {
	session.Store
	mu   sync.Mutex
	mode string // "", "before", "after", "invalid"; consumed by the next Append
}

func (f *faultStore) arm(mode string) {
	f.mu.Lock()
	f.mode = mode
	f.mu.Unlock()
}

func (f *faultStore) take() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	m := f.mode
	f.mode = ""
	return m
}

func (f *faultStore) Open(ctx context.Context, sid session.SessionID, opts session.OpenOptions) (session.Handle, error) {
	h, err := f.Store.Open(ctx, sid, opts)
	if err != nil {
		return nil, err
	}
	return &faultHandle{Handle: h, store: f}, nil
}

type faultHandle struct {
	session.Handle
	store *faultStore
}

var errInjected = errors.New("injected transport failure")

func (h *faultHandle) Append(ctx context.Context, p session.Proposal) (session.Commit, error) {
	switch h.store.take() {
	case "before":
		return session.Commit{}, errInjected
	case "invalid":
		return session.Commit{}, &session.Error{Code: session.ErrInvalid, Operation: "append", Detail: "injected validation rejection"}
	case "after":
		if _, err := h.Handle.Append(ctx, p); err != nil {
			return session.Commit{}, err
		}
		return session.Commit{}, errInjected // durable, but the response is lost
	}
	return h.Handle.Append(ctx, p)
}

func TestWriterReconcilesClaimsAfterAppendFailure(t *testing.T) {
	for _, tc := range []struct {
		mode       string
		beforeOpen artifact.ClaimState
		afterOpen  artifact.ClaimState
		committed  bool
	}{
		{"after", artifact.ClaimActive, artifact.ClaimActive, true},
		{"before", artifact.ClaimActive, artifact.ClaimReleased, false},
		{"invalid", artifact.ClaimReleased, artifact.ClaimReleased, false},
	} {
		t.Run(tc.mode, func(t *testing.T) {
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
			seg := tipSegment(t, f.store, "s")
			claimID := DeriveClaimID(seg, "c1", set.RefSetDigest)
			fs := &faultStore{Store: f.store}
			w, err := OpenWriter(ctx, fs, f.registry, f.admission(), "s", session.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			group := func(View) (*SemanticGroup, error) {
				return &SemanticGroup{CommitID: "c1", Batches: noteBatch(TypedEvent{Type: tpfx("a") + "note", Value: notePayload{Text: "file", Refs: []string{"b1"}}})}, nil
			}
			assertClaim := func(want artifact.ClaimState) {
				t.Helper()
				claim, ok, err := f.ledger.LookupClaim(ctx, claimID)
				if err != nil || !ok || claim.State != want {
					t.Fatalf("claim = %+v, %v, %v; want %s", claim, ok, err, want)
				}
			}
			fs.arm(tc.mode)
			if _, err := w.Commit(ctx, group); err == nil {
				t.Fatal("faulted append succeeded")
			}
			assertClaim(tc.beforeOpen)
			if tc.mode != "invalid" {
				if _, err := artifact.Reconcile(ctx, f.ledger, artifact.ClaimOwnerScope{Kind: ClaimOwnerKind, Authority: string(tipSegment(t, f.store, "s"))}, w); !errors.Is(err, &extension.Error{Code: extension.ErrUnknownOutcome}) {
					t.Fatalf("reconcile against failed writer = %v, want unknown_outcome", err)
				}
				assertClaim(artifact.ClaimActive)
			}
			if err := w.Close(ctx); err != nil {
				t.Fatal(err)
			}
			w, err = OpenWriter(ctx, fs, f.registry, f.admission(), "s", session.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close(ctx)
			assertClaim(tc.afterOpen)
			if exists, err := w.OwnerExists(ctx, CommitOwner(seg, "c1")); err != nil || exists != tc.committed {
				t.Fatalf("owner exists = %v, %v; want %v", exists, err, tc.committed)
			}
			if tc.committed {
				if res, err := w.Commit(ctx, group); err != nil || res.Outcome != CommitAlreadyApplied {
					t.Fatalf("committed commit replay = %+v, %v", res, err)
				}
				assertClaim(artifact.ClaimActive)
			} else {
				res, err := w.Commit(ctx, group)
				if err != nil || res.Outcome != CommitApplied || res.Claim == nil || res.Claim.ID != nextClaimID(claimID) || res.Claim.State != artifact.ClaimActive {
					t.Fatalf("unwritten commit replay = %+v, %v", res, err)
				}
				assertClaim(artifact.ClaimReleased)
			}
		})
	}
}

// EXT-WRT-4(b): an Append whose outcome is unknown fails the Writer closed. A
// reopened Writer replays the same commit and the kernel's index answers:
// AlreadyApplied when the commit reached the log, Applied when it did not. In
// both cases the log stays a valid chain with contiguous Seqs.
func TestWriterFailsClosedWhenAppendOutcomeUnknown(t *testing.T) {
	ctx := context.Background()
	base := newFixture(t)
	fs := &faultStore{Store: base.store}
	open := func(takeover bool) Writer {
		t.Helper()
		w, err := OpenWriter(ctx, fs, base.registry, base.admission(), "s", session.OpenOptions{Takeover: takeover})
		if err != nil {
			t.Fatalf("open writer: %v", err)
		}
		return w
	}
	unknown := &extension.Error{Code: extension.ErrUnknownOutcome}

	w1 := open(false)
	if res, err := w1.Commit(ctx, noteGroup("c1", "one")); err != nil || res.Outcome != CommitApplied {
		t.Fatalf("c1 = %+v %v", res, err)
	}

	// Durable append, lost response.
	fs.arm("after")
	if _, err := w1.Commit(ctx, noteGroup("c2", "two")); !errors.Is(err, unknown) {
		t.Fatalf("commit after lost response = %v, want unknown_outcome", err)
	}
	if _, err := w1.Commit(ctx, noteGroup("c3", "three")); !errors.Is(err, unknown) {
		t.Fatalf("writer did not stay failed: %v", err)
	}
	if page, _ := base.store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s"}); len(page.Commits) != 2 {
		t.Fatalf("log has %d commits after the lost-response append, want 2", len(page.Commits))
	}

	// Reopen: the replay is answered by the kernel, the next commit continues
	// from the real head.
	_ = w1.Close(ctx)
	w2 := open(true)
	res, err := w2.Commit(ctx, noteGroup("c2", "two"))
	if err != nil || res.Outcome != CommitAlreadyApplied || res.Commit.Seq != 1 {
		t.Fatalf("replay of the durable commit = %+v %v, want already_applied at seq 1", res, err)
	}
	res, err = w2.Commit(ctx, noteGroup("c3", "three"))
	if err != nil || res.Outcome != CommitApplied || res.Commit.Seq != 2 {
		t.Fatalf("next commit = %+v %v, want applied at seq 2", res, err)
	}
	if got := notes(t, w2); len(got) != 3 || got[0] != "one" || got[1] != "two" || got[2] != "three" {
		t.Fatalf("notes after reopen = %v", got)
	}

	// Failure before any write: the reopened Writer applies the commit.
	fs.arm("before")
	if _, err := w2.Commit(ctx, noteGroup("c4", "four")); !errors.Is(err, unknown) {
		t.Fatalf("commit after pre-write failure = %v, want unknown_outcome", err)
	}
	_ = w2.Close(ctx)
	w3 := open(true)
	res, err = w3.Commit(ctx, noteGroup("c4", "four"))
	if err != nil || res.Outcome != CommitApplied || res.Commit.Seq != 3 {
		t.Fatalf("replay of the unwritten commit = %+v %v, want applied at seq 3", res, err)
	}

	// A context error is returned before the kernel writes, so it is a known
	// outcome and does not fail the Writer.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := w3.Commit(cancelled, noteGroup("c5", "five")); !errors.Is(err, context.Canceled) {
		t.Fatalf("commit with cancelled ctx = %v, want context.Canceled", err)
	}
	if res, err := w3.Commit(ctx, noteGroup("c5", "five")); err != nil || res.Outcome != CommitApplied || res.Commit.Seq != 4 {
		t.Fatalf("commit after a cancelled attempt = %+v %v, want applied at seq 4", res, err)
	}

	page, err := base.store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s"})
	if err != nil || len(page.Commits) != 5 {
		t.Fatalf("final log = %d commits %v, want 5", len(page.Commits), err)
	}
	for i := range page.Commits {
		if page.Commits[i].Seq != session.CommitSeq(i) {
			t.Fatalf("seq at position %d is %d", i, page.Commits[i].Seq)
		}
	}
}
