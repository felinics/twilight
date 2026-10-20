package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/compaction"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"
)

type skewWorkReader struct {
	heads map[module.ProjectionID]ledger.CommitSeq
}

func (r skewWorkReader) Load(_ context.Context, _ session.SessionID, id module.ProjectionID, _ module.ProjectionVersion) (any, ledger.Head, error) {
	var state any
	switch id {
	case sessionstore.MachineProjectionID:
		state = sessionstore.Machine{}
	case chatlog.PendingProjectionID:
		state = chatlog.Pending{}
	case compaction.ProjectionID:
		state = compaction.State{}
	}
	return state, ledger.Head{Next: r.heads[id]}, nil
}
func TestInspectRetainsMinimumRevisionForOrdinaryReaders(t *testing.T) {
	for _, stale := range []module.ProjectionID{sessionstore.MachineProjectionID, chatlog.PendingProjectionID, compaction.ProjectionID} {
		t.Run(string(stale), func(t *testing.T) {
			r := skewWorkReader{heads: map[module.ProjectionID]ledger.CommitSeq{sessionstore.MachineProjectionID: 10, chatlog.PendingProjectionID: 10, compaction.ProjectionID: 10}}
			r.heads[stale] = 4
			work, err := Inspect(context.Background(), r, "session")
			if err != nil || work.Revision != 4 || work.Ready || work.Pending {
				t.Fatalf("work=%+v error=%v", work, err)
			}
		})
	}
}

type inspectTestReader func(module.ProjectionID) (any, ledger.Head, error)

func (r inspectTestReader) Load(_ context.Context, _ session.SessionID, id module.ProjectionID, _ module.ProjectionVersion) (any, ledger.Head, error) {
	return r(id)
}

func TestInspectUsesPendingInputsWithoutChatHistory(t *testing.T) {
	for _, queued := range []bool{false, true} {
		pending := chatlog.Pending{}
		if queued {
			pending.Inputs = pending.Inputs.Set("in", chatlog.PendingInput{ID: "in"})
		}
		reader := inspectTestReader(func(id module.ProjectionID) (any, ledger.Head, error) {
			var state any
			switch id {
			case sessionstore.MachineProjectionID:
				state = sessionstore.Machine{}
			case chatlog.PendingProjectionID:
				state = pending
			case compaction.ProjectionID:
				state = compaction.State{}
			default:
				t.Fatalf("inspection read full history or unknown projection %s", id)
			}
			return state, ledger.Head{Next: 7}, nil
		})
		work, err := Inspect(context.Background(), reader, "s")
		if err != nil || work.Ready != queued || work.Pending || work.Revision != 7 {
			t.Fatalf("work=%+v error=%v queued=%v", work, err, queued)
		}
	}
}

func TestInspectPendingReadFailures(t *testing.T) {
	failure := errors.New("pending unavailable")
	for _, broken := range []bool{false, true} {
		reader := inspectTestReader(func(id module.ProjectionID) (any, ledger.Head, error) {
			switch id {
			case sessionstore.MachineProjectionID:
				return sessionstore.Machine{}, ledger.Head{}, nil
			case chatlog.PendingProjectionID:
				if broken {
					return chatlog.Surface{}, ledger.Head{}, nil
				}
				return nil, ledger.Head{}, failure
			case compaction.ProjectionID:
				return compaction.State{}, ledger.Head{}, nil
			}
			t.Fatalf("unexpected projection %s", id)
			return nil, ledger.Head{}, nil
		})
		_, err := Inspect(context.Background(), reader, "s")
		if err == nil || (!broken && !errors.Is(err, failure)) {
			t.Fatalf("error=%v broken=%v", err, broken)
		}
	}
}
