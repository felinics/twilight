package chatlog_test

import (
	"context"
	"testing"

	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"
)

func TestPendingCheckpointCannotSilentlyHideQueuedWork(t *testing.T) {
	ctx := context.Background()
	reg, err := module.BuildRegistry(chatlog.Module, sessionstore.Module)
	if err != nil {
		t.Fatal(err)
	}
	store := filestoretest.Store(t)
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: "pending"}); err != nil {
		t.Fatal(err)
	}
	h, err := store.Open(ctx, "pending", session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(ctx)
	wire, err := reg.Encode(chatlog.TypeInputSubmitted, chatlog.InputSubmittedPayload{
		InputID: "in", Content: jsonstable.MustParse(`{"text":"hello"}`), Placement: chatlog.PlacementNextTurn, SubmittedAtUnixMilli: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Append(ctx, ledger.Proposal{CommitID: "submit", Batches: []ledger.EventBatch{{Domain: chatlog.Stream, Events: []ledger.Event{{Type: chatlog.TypeInputSubmitted, Payload: wire}}}}}); err != nil {
		t.Fatal(err)
	}
	pending, err := chatlog.ReadPending(ctx, session.NewProjectionReader(store, reg, nil), "pending")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := chatlog.PendingProjection.StateCodec.Encode(pending)
	if err != nil {
		t.Fatal(err)
	}
	head := h.Head()
	sealed, err := module.SealCheckpoint(chatlog.PendingProjectionID, chatlog.PendingProjection.Version, head, encoded)
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]jsonstable.Value{
		"valid":          sealed,
		"unsealed_empty": jsonstable.MustParse(`{"inputs":{}}`),
		"bad_digest":     jsonstable.MustParse(`{"state":{"inputs":{}},"digest":"sha256:invalid"}`),
	} {
		t.Run(name, func(t *testing.T) {
			cache := session.NewMemoryProjectionCache()
			if err := cache.Save(ctx, "pending", chatlog.PendingProjectionID, chatlog.PendingProjection.Version, value, head); err != nil {
				t.Fatal(err)
			}
			got, err := chatlog.ReadPending(ctx, session.NewProjectionReader(store, reg, cache), "pending")
			if err != nil || got.Inputs.Len() != 1 || !got.Inputs.Has("in") {
				t.Fatalf("queued work hidden: %+v error=%v", got, err)
			}
		})
	}
	// Valid cache plus a durable tail must observe the withdrawal.
	cache := session.NewMemoryProjectionCache()
	if err := cache.Save(ctx, "pending", chatlog.PendingProjectionID, chatlog.PendingProjection.Version, sealed, head); err != nil {
		t.Fatal(err)
	}
	wire, err = reg.Encode(chatlog.TypeInputWithdrawn, chatlog.InputWithdrawnPayload{InputID: "in", Reason: "withdrawn"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Append(ctx, ledger.Proposal{CommitID: "withdraw", Batches: []ledger.EventBatch{{Domain: chatlog.Stream, Events: []ledger.Event{{Type: chatlog.TypeInputWithdrawn, Payload: wire}}}}}); err != nil {
		t.Fatal(err)
	}
	got, err := chatlog.ReadPending(ctx, session.NewProjectionReader(store, reg, cache), "pending")
	if err != nil || got.Inputs.Len() != 0 {
		t.Fatalf("tail ignored: %+v error=%v", got, err)
	}
}
