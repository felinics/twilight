package filestore

import (
	"context"
	"encoding/json"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
	"os"
	"path/filepath"
	"strconv"
)

// projectionsDir holds cached projection states beside the session's authoritative
// files. Everything under it is derived data: losing it, or finding it corrupt
// or stale, only costs a reader a longer fold (EXT-PRJ-3).
const projectionsDir = "projections"

// ProjectionCache returns the durable session.ProjectionCache of this Store,
// so a process that reopens a Session starts its projections from the last
// cached entry instead of refolding the whole log.
func (s *Store) ProjectionCache() session.ProjectionCache { return projectionCache{s} }

// projectionRecord is the on-disk form of one cache entry. Through is the stream
// head the state was folded to; a reader revalidates it against the log before
// trusting State, so a record that is stale or ahead is simply ignored.
type projectionRecord struct {
	Through ledger.Head     `json:"through"`
	State   json.RawMessage `json:"state"`
}

type projectionCache struct{ store *Store }

func (c projectionCache) Load(_ context.Context, sid session.SessionID, id module.ProjectionID, v module.ProjectionVersion) (jsonstable.Value, ledger.Head, bool, error) {
	raw, err := os.ReadFile(projectionPath(c.store, sid, id, v))
	if err != nil {
		// A missing or unreadable entry is a cache miss, not a failure: the
		// reader falls back to folding from the beginning.
		return jsonstable.Value{}, ledger.Head{}, false, nil
	}
	var rec projectionRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return jsonstable.Value{}, ledger.Head{}, false, nil
	}
	state, err := jsonstable.Parse(rec.State)
	if err != nil {
		return jsonstable.Value{}, ledger.Head{}, false, nil
	}
	return state, rec.Through, true, nil
}

// Save keeps the entry monotonic (EXT-PRJ-7): a write that lands after a
// later one is dropped.
func (c projectionCache) Save(ctx context.Context, sid session.SessionID, id module.ProjectionID, v module.ProjectionVersion, state jsonstable.Value, through ledger.Head) error {
	if _, current, ok, _ := c.Load(ctx, sid, id, v); ok && current.Next >= through.Next {
		return nil
	}
	rec, err := json.Marshal(projectionRecord{Through: through, State: state.Bytes()})
	if err != nil {
		return err
	}
	path := projectionPath(c.store, sid, id, v)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	return writeAtomic(path, rec)
}

// projectionPath is <root>/sessions/<sid>/projections/<id>/<version>.json. A
// projection ID contains slashes, so it is percent-encoded exactly like a
// Session ID. The cache is the Session's, not the segment's: forks fold their
// own view of a shared prefix.
func projectionPath(s *Store, sid session.SessionID, id module.ProjectionID, v module.ProjectionVersion) string {
	return filepath.Join(s.sessionDir(sid), projectionsDir, encodeID(string(id)),
		strconv.FormatUint(uint64(v), 10)+".json")
}

var _ session.ProjectionCache = projectionCache{}
