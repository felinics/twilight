package postgres

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/felinics/twilight/agent/store/postgres/internal/db"
	"github.com/felinics/twilight/agentcore/artifact"
)

// ContentStoreOptions tune a ContentStore.
type ContentStoreOptions struct {
	// EphemeralTTL is how long an Ephemeral Put stays resolvable; zero
	// records no expiry on the Ref.
	EphemeralTTL time.Duration
	// MaxBytes bounds a Put; zero selects artifact.DefaultMaxContentBytes.
	MaxBytes int64
}

// ContentStore is the cas artifact.ContentStore of one Authority over the
// content table (ART-CAP-2): the frozen bodies every replica reads.
type ContentStore struct {
	d         *DB
	authority artifact.Authority
	opts      ContentStoreOptions
}

var _ artifact.ContentStore = (*ContentStore)(nil)

// Content is the cas content store of authority over this database.
func (d *DB) Content(authority artifact.Authority, opts ContentStoreOptions) (*ContentStore, error) {
	if authority == "" {
		return nil, &artifact.Error{Code: artifact.ErrInvalid, Operation: "content_store", Detail: "empty authority"}
	}
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = artifact.DefaultMaxContentBytes
	}
	return &ContentStore{d: d, authority: authority, opts: opts}, nil
}

// Authority is the store's Authority.
func (s *ContentStore) Authority() artifact.Authority { return s.authority }

const (
	opPut     = "put"
	opPromote = "promote"
)

type contentRow struct {
	data       []byte
	mediaType  string
	durability artifact.Durability
}

func (s *ContentStore) read(ctx context.Context, q *db.Queries, key artifact.Key) (contentRow, bool, error) {
	r, err := q.Content(ctx, db.ContentParams{Authority: string(s.authority), Key: string(key)})
	if noRows(err) {
		return contentRow{}, false, nil
	}
	if err != nil {
		return contentRow{}, false, err
	}
	return contentRow{data: r.Data, mediaType: r.MediaType, durability: artifact.Durability(r.Durability)}, true, nil
}

func (s *ContentStore) Put(ctx context.Context, req artifact.PutRequest) (artifact.Ref, error) {
	if req.Reader == nil {
		return artifact.Ref{}, &artifact.Error{Code: artifact.ErrInvalid, Operation: opPut, Detail: "nil reader"}
	}
	if req.Durability.Rank() < 0 {
		return artifact.Ref{}, &artifact.Error{Code: artifact.ErrInvalid, Operation: opPut, Detail: "unknown durability"}
	}
	data, err := io.ReadAll(io.LimitReader(req.Reader, min(s.opts.MaxBytes, math.MaxInt64-1)+1))
	if err != nil {
		return artifact.Ref{}, &artifact.Error{Code: artifact.ErrUnavailable, Operation: opPut, Detail: err.Error()}
	}
	if int64(len(data)) > s.opts.MaxBytes {
		return artifact.Ref{}, &artifact.Error{Code: artifact.ErrInvalid, Operation: opPut, Detail: fmt.Sprintf("content exceeds %d bytes", s.opts.MaxBytes)}
	}
	key, _ := artifact.CASKey(data)
	var stored contentRow
	err = s.d.tx(ctx, "content:"+string(s.authority)+"/"+string(key), func(q *db.Queries) error {
		existing, ok, err := s.read(ctx, q, key)
		if err != nil {
			return err
		}
		if ok {
			if !bytes.Equal(existing.data, data) {
				return &artifact.Error{Code: artifact.ErrCorrupt, Operation: opPut, Identity: string(key), Detail: "stored content differs from the new bytes under the same digest"}
			}
			if existing.mediaType != req.MediaType {
				return &artifact.Error{Code: artifact.ErrConflict, Operation: opPut, Identity: string(key), Detail: "same content declared with another media type"}
			}
			if req.Durability.Rank() > existing.durability.Rank() {
				existing.durability = req.Durability
				if err := q.UpdateContentDurability(ctx, db.UpdateContentDurabilityParams{Durability: string(req.Durability), Authority: string(s.authority), Key: string(key)}); err != nil {
					return err
				}
			}
			stored = existing
			return nil
		}
		stored = contentRow{data: data, mediaType: req.MediaType, durability: req.Durability}
		return q.InsertContent(ctx, db.InsertContentParams{Authority: string(s.authority), Key: string(key), Data: data, MediaType: req.MediaType, Durability: string(req.Durability)})
	})
	if err != nil {
		return artifact.Ref{}, err
	}
	return s.refFor(key, stored, req.Durability), nil
}

func (s *ContentStore) refFor(key artifact.Key, row contentRow, durability artifact.Durability) artifact.Ref {
	size := uint64(len(row.data))
	_, integrity := artifact.CASKey(row.data)
	ref := artifact.Ref{Scheme: artifact.SchemeCAS, Authority: s.authority, Key: key, MediaType: row.mediaType, SizeBytes: &size, Integrity: &integrity, Durability: durability}
	if durability == artifact.Ephemeral && s.opts.EphemeralTTL > 0 {
		at := s.d.now().Add(s.opts.EphemeralTTL).UnixMilli()
		ref.ExpiresAtUnixMilli = &at
	}
	return ref
}

func (s *ContentStore) locate(ctx context.Context, op string, ref artifact.Ref) (contentRow, error) {
	if err := ref.Validate(); err != nil {
		return contentRow{}, err
	}
	if ref.Scheme != artifact.SchemeCAS {
		return contentRow{}, &artifact.Error{Code: artifact.ErrUnsupported, Operation: op, Identity: string(ref.Key), Detail: fmt.Sprintf("scheme %s", ref.Scheme)}
	}
	if ref.Authority != s.authority {
		return contentRow{}, &artifact.Error{Code: artifact.ErrUnauthorized, Operation: op, Identity: string(ref.Key), Detail: fmt.Sprintf("authority %s is not %s", ref.Authority, s.authority)}
	}
	if ref.ExpiresAtUnixMilli != nil && s.d.now().UnixMilli() >= *ref.ExpiresAtUnixMilli {
		return contentRow{}, &artifact.Error{Code: artifact.ErrExpired, Operation: op, Identity: string(ref.Key)}
	}
	row, ok, err := s.read(ctx, s.d.q, ref.Key)
	if err != nil {
		return contentRow{}, err
	}
	if !ok {
		return contentRow{}, &artifact.Error{Code: artifact.ErrNotFound, Operation: op, Identity: string(ref.Key)}
	}
	key, integrity := artifact.CASKey(row.data)
	if key != ref.Key || *ref.Integrity != integrity {
		return contentRow{}, &artifact.Error{Code: artifact.ErrCorrupt, Operation: op, Identity: string(ref.Key), Detail: "stored bytes do not match the ref's integrity"}
	}
	if ref.SizeBytes != nil && *ref.SizeBytes != uint64(len(row.data)) {
		return contentRow{}, &artifact.Error{Code: artifact.ErrCorrupt, Operation: op, Identity: string(ref.Key), Detail: "stored size does not match the ref"}
	}
	if ref.MediaType != row.mediaType {
		return contentRow{}, &artifact.Error{Code: artifact.ErrCorrupt, Operation: op, Identity: string(ref.Key), Detail: "ref media type does not match the stored declaration"}
	}
	return row, nil
}

func (s *ContentStore) info(row contentRow, durability artifact.Durability) artifact.Info {
	size := uint64(len(row.data))
	_, integrity := artifact.CASKey(row.data)
	return artifact.Info{MediaType: row.mediaType, SizeBytes: &size, Integrity: &integrity, Durability: durability}
}

func (s *ContentStore) Stat(ctx context.Context, ref artifact.Ref) (artifact.Info, error) {
	row, err := s.locate(ctx, "stat", ref)
	if err != nil {
		return artifact.Info{}, err
	}
	return s.info(row, ref.Durability), nil
}

func (s *ContentStore) Open(ctx context.Context, ref artifact.Ref) (io.ReadCloser, artifact.Info, error) {
	row, err := s.locate(ctx, "open", ref)
	if err != nil {
		return nil, artifact.Info{}, err
	}
	return io.NopCloser(bytes.NewReader(row.data)), s.info(row, ref.Durability), nil
}

func (s *ContentStore) Promote(ctx context.Context, ref artifact.Ref, req artifact.PromoteRequest) (artifact.Ref, error) {
	if req.TargetScheme != artifact.SchemeCAS {
		return artifact.Ref{}, &artifact.Error{Code: artifact.ErrUnsupported, Operation: opPromote, Identity: string(ref.Key), Detail: fmt.Sprintf("target scheme %s", req.TargetScheme)}
	}
	if req.TargetAuthority != s.authority {
		return artifact.Ref{}, &artifact.Error{Code: artifact.ErrUnauthorized, Operation: opPromote, Identity: string(ref.Key), Detail: fmt.Sprintf("target authority %s is not %s", req.TargetAuthority, s.authority)}
	}
	if req.Durability.Rank() < 0 {
		return artifact.Ref{}, &artifact.Error{Code: artifact.ErrInvalid, Operation: opPromote, Identity: string(ref.Key), Detail: "unknown durability"}
	}
	if req.Durability.Rank() < ref.Durability.Rank() {
		return artifact.Ref{}, &artifact.Error{Code: artifact.ErrInvalid, Operation: opPromote, Identity: string(ref.Key), Detail: fmt.Sprintf("cannot lower durability from %s to %s", ref.Durability, req.Durability)}
	}
	row, err := s.locate(ctx, opPromote, ref)
	if err != nil {
		return artifact.Ref{}, err
	}
	if req.Durability.Rank() > row.durability.Rank() {
		err := s.d.tx(ctx, "content:"+string(s.authority)+"/"+string(ref.Key), func(q *db.Queries) error {
			current, ok, err := s.read(ctx, q, ref.Key)
			if err != nil {
				return err
			}
			if !ok {
				return &artifact.Error{Code: artifact.ErrNotFound, Operation: opPromote, Identity: string(ref.Key)}
			}
			if req.Durability.Rank() > current.durability.Rank() {
				return q.UpdateContentDurability(ctx, db.UpdateContentDurabilityParams{Durability: string(req.Durability), Authority: string(s.authority), Key: string(ref.Key)})
			}
			return nil
		})
		if err != nil {
			return artifact.Ref{}, err
		}
		row.durability = req.Durability
	}
	return s.refFor(ref.Key, row, req.Durability), nil
}
