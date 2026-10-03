package postgres

import (
	"context"
	"encoding/json"

	"github.com/felinics/twilight/agent/store/postgres/internal/db"
	"github.com/felinics/twilight/agentcore/inbox"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
)

// InboxStore is inbox.Store over the inbox table (APP-INB-1).
type InboxStore struct{ d *DB }

var _ inbox.Store = (*InboxStore)(nil)

// Inbox is the inbox.Store over this database.
func (d *DB) Inbox() *InboxStore { return &InboxStore{d: d} }

func inboxEntry(seq int64, commandID, kind, payload string, enqueuedAt int64, status, reason string, resolvedAt int64) (inbox.Entry, error) {
	e := inbox.Entry{Seq: uint64(seq), Command: inbox.Command{ID: inbox.CommandID(commandID), Kind: inbox.Kind(kind)}, EnqueuedAtUnixMilli: enqueuedAt} //nolint:gosec // G115: seq counts from 0
	if payload != "" {
		v, err := run.ParseCanonicalJSON([]byte(payload))
		if err != nil {
			return inbox.Entry{}, err
		}
		e.Command.Payload = v
	}
	if status != "" {
		e.Result = &inbox.Result{Status: inbox.Status(status), Reason: reason, ResolvedAtUnixMilli: resolvedAt}
	}
	return e, nil
}

func (s *InboxStore) lookup(ctx context.Context, q *db.Queries, sid session.SessionID, id inbox.CommandID) (inbox.Entry, bool, error) {
	r, err := q.InboxEntry(ctx, db.InboxEntryParams{Session: string(sid), CommandID: string(id)})
	if noRows(err) {
		return inbox.Entry{}, false, nil
	}
	if err != nil {
		return inbox.Entry{}, false, err
	}
	e, err := inboxEntry(r.Seq, r.CommandID, r.Kind, r.Payload, r.EnqueuedAt, r.Status, r.Reason, r.ResolvedAt)
	return e, err == nil, err
}

func (s *InboxStore) Enqueue(ctx context.Context, sid session.SessionID, c inbox.Command) (inbox.Entry, error) {
	var out inbox.Entry
	err := s.d.tx(ctx, "inbox:"+string(sid), func(q *db.Queries) error {
		existing, ok, err := s.lookup(ctx, q, sid, c.ID)
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
		next, err := q.InboxNextSeq(ctx, string(sid))
		if err != nil {
			return err
		}
		var payload string
		if !c.Payload.IsZero() {
			raw, err := json.Marshal(c.Payload)
			if err != nil {
				return err
			}
			payload = string(raw)
		}
		out = inbox.Entry{Seq: uint64(next), Command: c, EnqueuedAtUnixMilli: s.d.now().UnixMilli()} //nolint:gosec // G115: seq counts from 0
		return q.InsertInboxEntry(ctx, db.InsertInboxEntryParams{Session: string(sid), Seq: next, CommandID: string(c.ID), Kind: string(c.Kind), Payload: payload, EnqueuedAt: out.EnqueuedAtUnixMilli})
	})
	if err != nil {
		return inbox.Entry{}, err
	}
	return out, nil
}

func (s *InboxStore) Lookup(ctx context.Context, sid session.SessionID, id inbox.CommandID) (inbox.Entry, bool, error) {
	return s.lookup(ctx, s.d.q, sid, id)
}

func (s *InboxStore) Pending(ctx context.Context, sid session.SessionID) ([]inbox.Entry, error) {
	rows, err := s.d.q.InboxPending(ctx, string(sid))
	if err != nil {
		return nil, err
	}
	var out []inbox.Entry
	for _, r := range rows {
		e, err := inboxEntry(r.Seq, r.CommandID, r.Kind, r.Payload, r.EnqueuedAt, r.Status, r.Reason, r.ResolvedAt)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

func (s *InboxStore) Resolve(ctx context.Context, sid session.SessionID, seq uint64, r inbox.Result) error {
	n, err := s.d.q.ResolveInboxEntry(ctx, db.ResolveInboxEntryParams{Status: string(r.Status), Reason: r.Reason, ResolvedAt: s.d.now().UnixMilli(), Session: string(sid), Seq: int64(seq)}) //nolint:gosec // G115: seq values fit int64
	if err != nil {
		return err
	}
	if n == 0 {
		return inbox.ErrNotPending
	}
	return nil
}

func (s *InboxStore) Sessions(ctx context.Context, limit int) ([]session.SessionID, error) {
	ids, err := s.d.q.InboxSessions(ctx, pageLimit(limit))
	if err != nil {
		return nil, err
	}
	out := make([]session.SessionID, 0, len(ids))
	for _, id := range ids {
		out = append(out, session.SessionID(id))
	}
	return out, nil
}
