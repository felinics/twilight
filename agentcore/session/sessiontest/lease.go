package sessiontest

import (
	"context"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/session"
	"testing"
	"time"
)

// SES-OWN-1: a lease is live for LeaseDuration after Acquire and after each
// Renew. While live, an Open without Takeover is refused; once expired, an
// Open supersedes it and the old handle is fenced exactly as by Takeover.
// A zero duration never expires (the ownership case).
func testLease(t *testing.T, f Fixture) {
	if f.Advance == nil || f.Now == nil {
		t.Skip("fixture has no adjustable store clock")
	}
	ctx := context.Background()
	create(t, f.Store, "s")
	now := f.Now()
	opts := func(owner string) session.OpenOptions {
		return session.OpenOptions{Owner: owner, LeaseDuration: 10 * time.Second}
	}
	w1, err := f.Store.Open(ctx, "s", opts("a"))
	if err != nil {
		t.Fatal(err)
	}
	if l := w1.Lease(); l.Epoch != 1 || l.Owner != "a" || l.UntilUnixMilli != now.Add(10*time.Second).UnixMilli() {
		t.Fatalf("lease = %+v", l)
	}
	// SES-OWN-5: the lease is readable by anyone, as recorded, and changes
	// nothing. Before the first Open there was none; now there is w1's.
	if _, ok, err := f.Store.LeaseOf(ctx, "s"); err != nil || !ok {
		t.Fatalf("LeaseOf after open = ok:%v %v, want the lease", ok, err)
	}
	if _, _, err := f.Store.LeaseOf(ctx, "absent"); !session.IsNotFound(err) {
		t.Fatalf("LeaseOf of an unknown session = %v, want not found", err)
	}
	create(t, f.Store, "idle")
	if _, ok, err := f.Store.LeaseOf(ctx, "idle"); err != nil || ok {
		t.Fatalf("LeaseOf of a never-opened session = ok:%v %v, want none", ok, err)
	}
	if held, err := f.Store.ListLeases(ctx); err != nil || len(held) != 1 || held[0] != w1.Lease() {
		t.Fatalf("ListLeases = %+v %v, want w1's lease alone", held, err)
	}
	steps := []struct {
		name    string
		advance time.Duration
		renew   bool // renew w1 before advancing
		want    session.ErrorCode
	}{
		{name: "live", advance: 5 * time.Second, want: session.ErrOwned},
		{name: "renewed", renew: true, advance: 8 * time.Second, want: session.ErrOwned},
		{name: "expired", advance: 3 * time.Second, want: ""},
	}
	var w2 session.Handle
	for _, st := range steps {
		if st.renew {
			if err := w1.Renew(ctx); err != nil {
				t.Fatalf("%s: renew = %v", st.name, err)
			}
		}
		f.Advance(st.advance)
		now = f.Now()
		// ExpiredLeases judges by the store's clock (SES-OWN-6): a's lease
		// is in it exactly when it has expired, and a never-expiring one
		// never is.
		expired, err := f.Store.ExpiredLeases(ctx, 0)
		if err != nil || (len(expired) == 1) != (st.want == "") || (len(expired) == 1 && expired[0].Owner != "a") {
			t.Fatalf("%s: ExpiredLeases = %+v %v, want a's lease exactly when expired (%v)", st.name, expired, err, st.want == "")
		}
		if st.want == "" {
			// Expired and not yet superseded: the read still names a, and
			// the caller judges expiry against its own clock.
			if l, ok, err := f.Store.LeaseOf(ctx, "s"); err != nil || !ok || l.Owner != "a" || l.UntilUnixMilli > now.UnixMilli() {
				t.Fatalf("%s: LeaseOf before takeover = %+v ok:%v %v, want a's expired lease", st.name, l, ok, err)
			}
			if limited, err := f.Store.ExpiredLeases(ctx, 1); err != nil || len(limited) != 1 {
				t.Fatalf("%s: ExpiredLeases limited = %+v %v", st.name, limited, err)
			}
		}
		h, err := f.Store.Open(ctx, "s", opts("b"))
		if st.want != "" {
			if !session.IsCode(err, st.want) {
				t.Fatalf("%s: open = %v, want %s", st.name, err, st.want)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: open after expiry = %v, want success", st.name, err)
		}
		w2 = h
	}
	if w2.Epoch() != 2 || w2.Lease().Owner != "b" {
		t.Fatalf("lease after expiry = %+v, want epoch 2 owner b", w2.Lease())
	}
	if l, ok, err := f.Store.LeaseOf(ctx, "s"); err != nil || !ok || l != w2.Lease() {
		t.Fatalf("LeaseOf after takeover = %+v ok:%v %v, want b's lease", l, ok, err)
	}
	// The expired holder is fenced at Renew and at Append; its Close is a no-op.
	if err := w1.Renew(ctx); !session.IsOwnershipLost(err) {
		t.Fatalf("expired holder renew = %v, want ownership_lost", err)
	}
	if _, err := w1.Append(ctx, ledger.Proposal{CommitID: "late", Batches: []ledger.EventBatch{batch(chatStream(), "twilight/x/a", `{}`)}}); !session.IsOwnershipLost(err) {
		t.Fatalf("expired holder append = %v, want ownership_lost", err)
	}
	if err := w1.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Store.Open(ctx, "s", opts("c")); !session.IsOwned(err) {
		t.Fatal("closing a superseded holder released the current lease")
	}
	appendCommit(t, w2, "c1", batch(chatStream(), "twilight/x/a", `{}`))
	// A never-expiring lease over a released root, then Takeover of a live one.
	if err := w2.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := f.Store.LeaseOf(ctx, "s"); err != nil || ok {
		t.Fatalf("LeaseOf after release = ok:%v %v, want none", ok, err)
	}
	if held, err := f.Store.ListLeases(ctx); err != nil || len(held) != 0 {
		t.Fatalf("ListLeases after release = %+v %v, want none", held, err)
	}
	w3 := open(t, f.Store, "s", false)
	f.Advance(time.Hour)
	if _, err := f.Store.Open(ctx, "s", opts("d")); !session.IsOwned(err) {
		t.Fatalf("zero-duration lease expired: %v", err)
	}
	w4, err := f.Store.Open(ctx, "s", session.OpenOptions{Takeover: true, Owner: "d", LeaseDuration: time.Second})
	if err != nil || w4.Epoch() != w3.Epoch()+1 {
		t.Fatalf("takeover = %v epoch %d", err, w4.Epoch())
	}
	_ = w4.Close(ctx)
}
