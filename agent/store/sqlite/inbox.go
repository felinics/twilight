package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/felinics/twilight/agentcore/inbox"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
)

// InboxStore is inbox.Store over the inbox table: one row per command,
// Seq assigned per Session inside the enqueue transaction, the pending
// rows selected by an index on (status, session).
type InboxStore struct {
	db  *sql.DB
	now func() time.Time
}

var _ inbox.Store = (*InboxStore)(nil)

// Inbox is the inbox.Store over this database.
func (d *DB) Inbox() *InboxStore { return &InboxStore{db: d.db, now: d.now} }

func scanEntry(row interface{ Scan(...any) error }) (inbox.Entry, error) {
	var (
		e        inbox.Entry
		id, kind string
		payload  sql.NullString
		status   string
		reason   sql.NullString
		resolved sql.NullInt64
	)
	if err := row.Scan(&e.Seq, &id, &kind, &payload, &e.EnqueuedAtUnixMilli, &status, &reason, &resolved); err != nil {
		return inbox.Entry{}, err
	}
	e.Command = inbox.Command{ID: inbox.CommandID(id), Kind: inbox.Kind(kind)}
	if payload.Valid && payload.String != "" {
		v, err := run.ParseCanonicalJSON([]byte(payload.String))
		if err != nil {
			return inbox.Entry{}, err
		}
		e.Command.Payload = v
	}
	if status != "" {
		e.Result = &inbox.Result{Status: inbox.Status(status), Reason: reason.String, ResolvedAtUnixMilli: resolved.Int64}
	}
	return e, nil
}

const entryColumns = `seq, command_id, kind, payload, enqueued_at, status, reason, resolved_at`

func lookupEntry(ctx context.Context, q querier, sid session.SessionID, id inbox.CommandID) (inbox.Entry, bool, error) {
	e, err := scanEntry(q.QueryRowContext(ctx, `SELECT `+entryColumns+` FROM inbox WHERE session = ? AND command_id = ?`, string(sid), string(id)))
	if errors.Is(err, sql.ErrNoRows) {
		return inbox.Entry{}, false, nil
	}
	if err != nil {
		return inbox.Entry{}, false, err
	}
	return e, true, nil
}

func (s *InboxStore) Enqueue(ctx context.Context, sid session.SessionID, c inbox.Command) (inbox.Entry, error) {
	var out inbox.Entry
	err := tx(ctx, s.db, func(t *sql.Tx) error {
		existing, ok, err := lookupEntry(ctx, t, sid, c.ID)
		if err != nil {
			return err
		}
		if ok {
			if existing.Command.Kind != c.Kind || !existing.Command.Payload.Equal(c.Payload) {
				return inbox.ErrCommandConflict
			}
			out = existing
			return nil
		}
		var next sql.NullInt64
		if err := t.QueryRowContext(ctx, `SELECT MAX(seq) + 1 FROM inbox WHERE session = ?`, string(sid)).Scan(&next); err != nil {
			return err
		}
		var payload []byte
		if !c.Payload.IsZero() {
			payload, err = json.Marshal(c.Payload)
			if err != nil {
				return err
			}
		}
		out = inbox.Entry{Seq: uint64(next.Int64), Command: c, EnqueuedAtUnixMilli: s.now().UnixMilli()} //nolint:gosec // G115: seq counts from 0
		_, err = t.ExecContext(ctx, `INSERT INTO inbox (session, seq, command_id, kind, payload, enqueued_at, status) VALUES (?, ?, ?, ?, ?, ?, '')`,
			string(sid), out.Seq, string(c.ID), string(c.Kind), string(payload), out.EnqueuedAtUnixMilli)
		return err
	})
	if err != nil {
		return inbox.Entry{}, err
	}
	return out, nil
}

func (s *InboxStore) Lookup(ctx context.Context, sid session.SessionID, id inbox.CommandID) (inbox.Entry, bool, error) {
	return lookupEntry(ctx, s.db, sid, id)
}

func (s *InboxStore) Pending(ctx context.Context, sid session.SessionID) ([]inbox.Entry, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+entryColumns+` FROM inbox WHERE session = ? AND status = '' ORDER BY seq`, string(sid))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []inbox.Entry
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *InboxStore) Resolve(ctx context.Context, sid session.SessionID, seq uint64, r inbox.Result) error {
	res, err := s.db.ExecContext(ctx, `UPDATE inbox SET status = ?, reason = ?, resolved_at = ? WHERE session = ? AND seq = ? AND status = ''`,
		string(r.Status), r.Reason, s.now().UnixMilli(), string(sid), seq)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return inbox.ErrNotPending
	}
	return nil
}

func (s *InboxStore) Sessions(ctx context.Context, limit int) ([]session.SessionID, error) {
	if limit <= 0 {
		limit = -1 // SQLite: no limit
	}
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT session FROM inbox WHERE status = '' ORDER BY session LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []session.SessionID
	for rows.Next() {
		var sid string
		if err := rows.Scan(&sid); err != nil {
			return nil, err
		}
		out = append(out, session.SessionID(sid))
	}
	return out, rows.Err()
}
