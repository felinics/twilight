package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/felinics/twilight/agentcore/artifact"
)

// BindingStore is artifact.BindingStore over the bindings table (ART-BND-1):
// a BindingID maps to exactly one immutable Binding, and recreating it with a
// different digest is a conflict.
type BindingStore struct{ db *sql.DB }

var _ artifact.BindingStore = (*BindingStore)(nil)

func (s *BindingStore) CreateBinding(ctx context.Context, b artifact.Binding) (artifact.Binding, error) { //nolint:gocritic // hugeParam: BindingStore contract takes the Binding by value
	want, err := artifact.DigestBinding(b.ID, b.Ref)
	if err != nil {
		return artifact.Binding{}, err
	}
	if b.Digest != want {
		return artifact.Binding{}, &artifact.Error{Code: artifact.ErrInvalid, Operation: "create_binding", Identity: string(b.ID), Detail: "binding digest mismatch"}
	}
	raw, err := json.Marshal(b)
	if err != nil {
		return artifact.Binding{}, err
	}
	var out artifact.Binding
	err = tx(ctx, s.db, func(t *sql.Tx) error {
		existing, ok, err := lookupBinding(ctx, t, b.ID)
		if err != nil {
			return err
		}
		if ok {
			if existing.Digest != b.Digest {
				return &artifact.Error{Code: artifact.ErrConflict, Operation: "create_binding", Identity: string(b.ID)}
			}
			out = existing
			return nil
		}
		out = b
		_, err = t.ExecContext(ctx, `INSERT INTO bindings (id, digest, binding) VALUES (?, ?, ?)`, string(b.ID), string(b.Digest), string(raw))
		return err
	})
	if err != nil {
		return artifact.Binding{}, err
	}
	return out, nil
}

func lookupBinding(ctx context.Context, q querier, id artifact.BindingID) (artifact.Binding, bool, error) {
	var raw string
	err := q.QueryRowContext(ctx, `SELECT binding FROM bindings WHERE id = ?`, string(id)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return artifact.Binding{}, false, nil
	}
	if err != nil {
		return artifact.Binding{}, false, err
	}
	var b artifact.Binding
	if err := json.Unmarshal([]byte(raw), &b); err != nil {
		return artifact.Binding{}, false, &artifact.Error{Code: artifact.ErrCorrupt, Operation: "lookup_binding", Identity: string(id), Detail: err.Error()}
	}
	return b, true, nil
}

func (s *BindingStore) LookupBinding(ctx context.Context, id artifact.BindingID) (artifact.Binding, bool, error) {
	return lookupBinding(ctx, s.db, id)
}

func (s *BindingStore) ResolveBinding(ctx context.Context, id artifact.BindingID) (artifact.Binding, error) {
	b, ok, err := s.LookupBinding(ctx, id)
	if err != nil {
		return artifact.Binding{}, err
	}
	if !ok {
		return artifact.Binding{}, &artifact.Error{Code: artifact.ErrNotFound, Operation: "resolve_binding", Identity: string(id)}
	}
	return b, nil
}
