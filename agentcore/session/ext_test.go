package session_test

import (
	"context"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"testing"
)

// TestKernelExtRoundTrip creates a segment carrying Ext through a Store,
// reads the header back byte for byte under its module key and reopens the
// Session.
func TestKernelExtRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := filestoretest.Store(t)
	key := module.ModuleKey{Source: "acme", ID: "audit"}
	ext := module.Extensions{key: module.RawValue(`{"by":"later"}`)}
	same := func(got module.Extensions) bool { return len(got) == 1 && string(got[key]) == string(ext[key]) }
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: "s", Ext: ext}); err != nil {
		t.Fatal(err)
	}
	h, err := store.Open(ctx, "s", session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Append(ctx, ledger.Proposal{CommitID: "c1", Batches: []ledger.EventBatch{{Domain: ledger.Domain{Name: "chat"},
		Events: []ledger.Event{{Type: "twilight/x/a", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{}`)}}}}}); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	header, err := store.Header(ctx, "s")
	if err != nil || !same(header.Ext) {
		t.Fatalf("header = %+v, %v", header, err)
	}
	page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s"})
	if err != nil || len(page.Commits) != 1 {
		t.Fatalf("read = %+v, %v", page, err)
	}
	if h, err = store.Open(ctx, "s", session.OpenOptions{}); err != nil {
		t.Fatalf("reopen with ext: %v", err)
	}
	_ = h.Close(ctx)
}
