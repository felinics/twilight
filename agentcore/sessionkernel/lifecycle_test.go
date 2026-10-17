package sessionkernel_test

import (
	"context"
	"testing"

	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/sessionkernel"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"
)

func TestKernelCloseDoesNotCloseBoundWriterOrLease(t *testing.T) {
	ctx := context.Background()
	store := filestoretest.Store(t)
	bindings, retention := artifacttest.Stores(t)
	k, err := sessionkernel.New(sessionkernel.Ports{Store: store, Content: filestoretest.Content(t, sessionstore.FrozenAuthority), Artifacts: sessionkernel.Artifacts{Bindings: bindings, Ledger: retention}})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close(ctx)
	if err := k.CreateSession(ctx, "s", nil); err != nil {
		t.Fatal(err)
	}
	h, err := store.Open(ctx, "s", session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(ctx)
	w, err := k.Writers.Bind(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close(ctx)
	if err := k.Close(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := w.Commit(ctx, func(writer.View) (*writer.SemanticGroup, error) { return nil, nil })
	if err != nil || res.Outcome != writer.CommitNoop {
		t.Fatalf("Kernel.Close closed Writer: %+v %v", res, err)
	}
	if err := h.Renew(ctx); err != nil {
		t.Fatalf("Kernel.Close released lease: %v", err)
	}
}
