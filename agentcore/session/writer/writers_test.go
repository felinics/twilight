package writer

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
)

// EXT-WRT-6: a Writers value confines a failed Writer to that instance. After
// an unknown outcome or a Close the next Writer(sid) reopens from the log;
// after ownership loss it keeps answering ownership_lost until the host
// forgets the Session with CloseWriter, because reopening would take the
// Session back from its new owner.
func TestWritersReopenAfterFailure(t *testing.T) {
	ownershipLost := &extension.Error{Code: extension.ErrOwnershipLost}
	cases := []struct {
		name string
		lose func(t *testing.T, f *fixture, fs *faultStore, w Writer)
		// stays is the error Writer(sid) keeps returning until CloseWriter; nil
		// means the next Writer(sid) already reopens.
		stays error
	}{
		{"unknown outcome", func(t *testing.T, _ *fixture, fs *faultStore, w Writer) {
			fs.arm("after")
			if _, err := w.Commit(context.Background(), noteGroup("c2", "two")); !errors.Is(err, &extension.Error{Code: extension.ErrUnknownOutcome}) {
				t.Fatalf("commit with a lost response = %v, want unknown_outcome", err)
			}
		}, nil},
		{"closed", func(t *testing.T, _ *fixture, _ *faultStore, w Writer) {
			if err := w.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
		}, nil},
		{"ownership lost", func(t *testing.T, f *fixture, _ *faultStore, w Writer) {
			other := f.open(t, true) // another process takes the Session over
			defer other.Close(context.Background())
			if _, err := w.Commit(context.Background(), noteGroup("c2", "two")); !errors.Is(err, ownershipLost) {
				t.Fatalf("commit after takeover = %v, want ownership_lost", err)
			}
		}, ownershipLost},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			f := newFixture(t)
			fs := &faultStore{Store: f.store}
			ws := NewWriters(fs, f.registry, f.admission(), session.OpenOptions{Takeover: true}, WritersConfig{})
			defer CloseWriters(ctx, ws)
			w, err := ws.Writer(ctx, "s")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := w.Commit(ctx, noteGroup("c1", "one")); err != nil {
				t.Fatal(err)
			}
			tc.lose(t, f, fs, w)
			if _, err := w.Commit(ctx, noteGroup("c3", "three")); err == nil {
				t.Fatal("failed writer accepted a commit")
			}
			if tc.stays != nil {
				if _, err := ws.Writer(ctx, "s"); !errors.Is(err, tc.stays) {
					t.Fatalf("Writer after the loss = %v, want %v", err, tc.stays)
				}
				if err := CloseWriter(ctx, ws, "s"); err != nil {
					t.Fatalf("CloseWriter = %v", err)
				}
			}
			again, err := ws.Writer(ctx, "s")
			if err != nil {
				t.Fatalf("Writer after %s = %v, want a reopened Writer", tc.name, err)
			}
			if again == w {
				t.Fatal("Writers handed out the failed instance again")
			}
			res, err := again.Commit(ctx, noteGroup("c3", "three"))
			if err != nil || res.Outcome != CommitApplied {
				t.Fatalf("commit on the reopened writer = %+v %v", res, err)
			}
			if got := notes(t, again); got[len(got)-1] != "three" {
				t.Fatalf("notes after reopen = %v", got)
			}
		})
	}
}
