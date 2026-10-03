package postgres

import (
	"context"
	"encoding/json"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/jackc/pgx/v5"
)

// SeedSegmentCommits bulk-loads commits into a segment for benchmarks: the
// rows Append would write, without the per-commit transaction.
func (d *DB) SeedSegmentCommits(ctx context.Context, segment session.SegmentID, commits []ledger.Commit) error {
	rows := make([][]any, 0, len(commits))
	var streamRows [][]any
	for i := range commits {
		c := &commits[i]
		body, err := json.Marshal(c)
		if err != nil {
			return err
		}
		rows = append(rows, []any{string(segment), int64(c.Seq), string(c.CommitID), string(body)}) //nolint:gosec // G115: seq values fit int64
		for _, sc := range session.IndexEntryOf(c).Streams {
			streamRows = append(streamRows, []any{string(segment), int64(c.Seq), sc.Domain.Name, sc.Domain.Id, int64(sc.Events)}) //nolint:gosec // G115: seq values fit int64
		}
	}
	if _, err := d.pool.CopyFrom(ctx, pgx.Identifier{"session_commits"}, []string{"segment", "seq", "commit_id", "body"}, pgx.CopyFromRows(rows)); err != nil {
		return err
	}
	_, err := d.pool.CopyFrom(ctx, pgx.Identifier{"session_commit_streams"}, []string{"segment", "seq", "domain", "stream_id", "events"}, pgx.CopyFromRows(streamRows))
	return err
}

// IndexOf is the backend's Index, exposed for benchmarks.
func (s *SessionStore) IndexOf(ctx context.Context, segment session.SegmentID) (session.CommitIndex, ledger.Head, error) {
	return s.backend.Index(ctx, segment)
}

// DeleteRecord, TruncateSegment and CreateSession expose the backend writes
// the retention tests call directly. Production goes through the Ledger.
func (s *SessionStore) DeleteRecord(ctx context.Context, sid session.SessionID) (session.SessionRecord, error) {
	return s.backend.DeleteRecord(ctx, sid)
}

func (s *SessionStore) TruncateSegment(ctx context.Context, id session.SegmentID, through ledger.CommitSeq) (ledger.Head, []ledger.CommitID, error) {
	return s.backend.TruncateSegment(ctx, id, through)
}

func (s *SessionStore) CreateSession(ctx context.Context, seg session.Segment, rec session.SessionRecord) error {
	return s.backend.CreateSession(ctx, seg, rec)
}
