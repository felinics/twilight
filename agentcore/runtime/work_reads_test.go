package runtime

import (
	"context"
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
	case chatlog.SurfaceProjectionID:
		state = chatlog.Surface{}
	case compaction.ProjectionID:
		state = compaction.State{}
	}
	return state, ledger.Head{Next: r.heads[id]}, nil
}
func TestInspectRetainsMinimumRevisionForOrdinaryReaders(t *testing.T) {
	for _, stale := range []module.ProjectionID{sessionstore.MachineProjectionID, chatlog.SurfaceProjectionID, compaction.ProjectionID} {
		t.Run(string(stale), func(t *testing.T) {
			r := skewWorkReader{heads: map[module.ProjectionID]ledger.CommitSeq{sessionstore.MachineProjectionID: 10, chatlog.SurfaceProjectionID: 10, compaction.ProjectionID: 10}}
			r.heads[stale] = 4
			work, err := Inspect(context.Background(), r, "session")
			if err != nil || work.Revision != 4 || work.Ready || work.Pending {
				t.Fatalf("work=%+v error=%v", work, err)
			}
		})
	}
}
