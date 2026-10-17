package executor_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/felinics/twilight/agentcore/executor"
	executionstore "github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/executor/store/storetest"
	"github.com/felinics/twilight/agentcore/turn/effect"
)

// Embedding only Store deliberately hides the optional Directory.
type perKeyOnlyStore struct{ executionstore.Store }

type directoryStore struct {
	executionstore.Store
	page  effect.OutstandingPage
	err   error
	after effect.AssignmentKey
	limit int
}

func (s *directoryStore) ListOutstanding(_ context.Context, after effect.AssignmentKey, limit int) (effect.OutstandingPage, error) {
	s.after, s.limit = after, limit
	return s.page, s.err
}

func TestWorkerOutstandingCapability(t *testing.T) {
	ctx := context.Background()
	w, err := executor.NewWorker(ctx, perKeyOnlyStore{storetest.NewMap(nil)}, routes(newTestBackend()))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.ListOutstanding(ctx, effect.AssignmentKey{}, 10); !errors.Is(err, effect.ErrOutstandingUnsupported) {
		t.Fatalf("missing Directory returned %v, want capability error", err)
	}
	key := effect.AssignmentKey{Session: "session", OwnerKind: effect.OwnerTurn, OwnerID: string("run"), Effect: "effect"}
	s := &directoryStore{Store: storetest.NewMap(nil), page: effect.OutstandingPage{Items: []effect.ExecutionSummary{{Key: key, Status: effect.ExecutionAccepted, Revision: 7}}, Next: key, More: true}}
	w2, err := executor.NewWorker(ctx, s, routes(newTestBackend()))
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	page, err := w2.ListOutstanding(ctx, key, 17)
	if err != nil || !reflect.DeepEqual(page, s.page) || s.after != key || s.limit != 17 {
		t.Fatalf("delegation page=%+v err=%v after=%+v limit=%d", page, err, s.after, s.limit)
	}
	s.err = errors.New("directory unavailable")
	if _, err := w2.ListOutstanding(ctx, key, 17); !errors.Is(err, s.err) {
		t.Fatalf("directory error lost: %v", err)
	}
}
