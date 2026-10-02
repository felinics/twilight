package filestore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/felinics/twilight/agentcore/artifact"
)

// casDir is the content store directory under the root. The leading "%"
// keeps it disjoint from every session directory: encodeID only ever emits
// "%" as part of a %XX escape, so no SessionID encodes to a name starting
// with "%c".
const casDir = "%cas"

// ContentStoreOptions tunes the file-backed cas store; the zero value matches
// artifact.MemoryContentStoreOptions.
type ContentStoreOptions struct {
	// Now is the clock ephemeral expiry is judged by; nil selects time.Now.
	Now func() time.Time
	// EphemeralTTL is how long an Ephemeral Put stays resolvable; zero records
	// no expiry on the Ref.
	EphemeralTTL time.Duration
	// MaxBytes caps one Put; zero means artifact.DefaultMaxContentBytes.
	MaxBytes int64
}

// ContentStore is the file-backed artifact.ContentStore of one Authority for
// the cas scheme: <root>/%cas/<authority>/<key> holds the bytes and
// <key>.meta the media type and the highest durability any Ref reached.
// Both files are written atomically, bytes before meta, so a crash between
// the two leaves an entry the store treats as absent and the next Put
// completes. Two instances over one root see each other's content, which is
// what lets a restarted process read the frozen request of an interrupted
// ModelStep (RUN-WIR-4).
// Operation names of the content store's errors.
const (
	opPut     = "put"
	opPromote = "promote"
)

type ContentStore struct {
	dir       string
	authority artifact.Authority
	opts      ContentStoreOptions
	mu        sync.Mutex
}

type contentMeta struct {
	MediaType  string              `json:"mediaType"`
	Durability artifact.Durability `json:"durability"`
}

// NewContentStore opens (creating if needed) the cas store of authority under
// root. The root may be shared with New's session store.
func NewContentStore(root string, authority artifact.Authority, opts ContentStoreOptions) (*ContentStore, error) {
	if root == "" {
		return nil, errors.New("filestore: content store: empty root")
	}
	if authority == "" {
		return nil, &artifact.Error{Code: artifact.ErrInvalid, Operation: "content_store", Detail: "empty authority"}
	}
	dir := filepath.Join(root, casDir, encodeID(string(authority)))
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("filestore: content store: %w", err)
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = artifact.DefaultMaxContentBytes
	}
	return &ContentStore{dir: dir, authority: authority, opts: opts}, nil
}

// Authority is the logical store instance this store serves.
func (s *ContentStore) Authority() artifact.Authority { return s.authority }

func (s *ContentStore) paths(key artifact.Key) (data, meta string) {
	base := filepath.Join(s.dir, encodeID(string(key)))
	return base, base + ".meta"
}

// read returns the entry for key; absent when either file is missing.
func (s *ContentStore) read(key artifact.Key) ([]byte, contentMeta, bool, error) { //nolint:gocritic // unnamedResult: the four results are documented above
	dataPath, metaPath := s.paths(key)
	rawMeta, err := os.ReadFile(metaPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, contentMeta{}, false, nil
	}
	if err != nil {
		return nil, contentMeta{}, false, err
	}
	var meta contentMeta
	if err := json.Unmarshal(rawMeta, &meta); err != nil {
		return nil, contentMeta{}, false, &artifact.Error{Code: artifact.ErrCorrupt, Operation: "read", Identity: string(key), Detail: err.Error()}
	}
	data, err := os.ReadFile(dataPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, contentMeta{}, false, nil
	}
	if err != nil {
		return nil, contentMeta{}, false, err
	}
	return data, meta, true, nil
}

func (s *ContentStore) writeMeta(key artifact.Key, meta contentMeta) error {
	_, metaPath := s.paths(key)
	raw, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return writeAtomic(metaPath, raw)
}

func (s *ContentStore) Put(ctx context.Context, req artifact.PutRequest) (artifact.Ref, error) {
	if err := ctx.Err(); err != nil {
		return artifact.Ref{}, err
	}
	if req.Reader == nil {
		return artifact.Ref{}, &artifact.Error{Code: artifact.ErrInvalid, Operation: opPut, Detail: "nil reader"}
	}
	if req.Durability.Rank() < 0 {
		return artifact.Ref{}, &artifact.Error{Code: artifact.ErrInvalid, Operation: opPut, Detail: "unknown durability"}
	}
	// One byte past the cap tells an oversized body from one at the cap; the
	// min keeps that byte representable when MaxBytes is MaxInt64.
	data, err := io.ReadAll(io.LimitReader(req.Reader, min(s.opts.MaxBytes, math.MaxInt64-1)+1))
	if err != nil {
		return artifact.Ref{}, &artifact.Error{Code: artifact.ErrUnavailable, Operation: opPut, Detail: err.Error()}
	}
	if int64(len(data)) > s.opts.MaxBytes {
		return artifact.Ref{}, &artifact.Error{Code: artifact.ErrInvalid, Operation: opPut, Detail: fmt.Sprintf("content exceeds %d bytes", s.opts.MaxBytes)}
	}
	key, _ := artifact.CASKey(data)
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, meta, ok, err := s.read(key)
	if err != nil {
		return artifact.Ref{}, err
	}
	if ok {
		if !bytes.Equal(existing, data) {
			return artifact.Ref{}, &artifact.Error{Code: artifact.ErrCorrupt, Operation: opPut, Identity: string(key), Detail: "stored content differs from the new bytes under the same digest"}
		}
		if meta.MediaType != req.MediaType {
			return artifact.Ref{}, &artifact.Error{Code: artifact.ErrConflict, Operation: opPut, Identity: string(key), Detail: "same content declared with another media type"}
		}
		if req.Durability.Rank() > meta.Durability.Rank() {
			meta.Durability = req.Durability
			if err := s.writeMeta(key, meta); err != nil {
				return artifact.Ref{}, err
			}
		}
	} else {
		dataPath, _ := s.paths(key)
		if err := writeAtomic(dataPath, data); err != nil {
			return artifact.Ref{}, err
		}
		meta = contentMeta{MediaType: req.MediaType, Durability: req.Durability}
		if err := s.writeMeta(key, meta); err != nil {
			return artifact.Ref{}, err
		}
	}
	return s.refFor(key, data, meta, req.Durability), nil
}

// refFor builds the Ref of an entry at durability; an Ephemeral Ref carries
// the expiry the options prescribe.
func (s *ContentStore) refFor(key artifact.Key, data []byte, meta contentMeta, durability artifact.Durability) artifact.Ref {
	size := uint64(len(data))
	_, integrity := artifact.CASKey(data)
	ref := artifact.Ref{Scheme: artifact.SchemeCAS, Authority: s.authority, Key: key, MediaType: meta.MediaType, SizeBytes: &size, Integrity: &integrity, Durability: durability}
	if durability == artifact.Ephemeral && s.opts.EphemeralTTL > 0 {
		at := s.opts.Now().Add(s.opts.EphemeralTTL).UnixMilli()
		ref.ExpiresAtUnixMilli = &at
	}
	return ref
}

// locate is the shared Resolver check: scheme, authority, presence, expiry
// and agreement between the Ref's declarations and the stored bytes.
func (s *ContentStore) locate(op string, ref artifact.Ref) ([]byte, contentMeta, error) {
	if err := ref.Validate(); err != nil {
		return nil, contentMeta{}, err
	}
	if ref.Scheme != artifact.SchemeCAS {
		return nil, contentMeta{}, &artifact.Error{Code: artifact.ErrUnsupported, Operation: op, Identity: string(ref.Key), Detail: fmt.Sprintf("scheme %s", ref.Scheme)}
	}
	if ref.Authority != s.authority {
		return nil, contentMeta{}, &artifact.Error{Code: artifact.ErrUnauthorized, Operation: op, Identity: string(ref.Key), Detail: fmt.Sprintf("authority %s is not %s", ref.Authority, s.authority)}
	}
	if ref.ExpiresAtUnixMilli != nil && s.opts.Now().UnixMilli() >= *ref.ExpiresAtUnixMilli {
		return nil, contentMeta{}, &artifact.Error{Code: artifact.ErrExpired, Operation: op, Identity: string(ref.Key)}
	}
	s.mu.Lock()
	data, meta, ok, err := s.read(ref.Key)
	s.mu.Unlock()
	if err != nil {
		return nil, contentMeta{}, err
	}
	if !ok {
		return nil, contentMeta{}, &artifact.Error{Code: artifact.ErrNotFound, Operation: op, Identity: string(ref.Key)}
	}
	key, integrity := artifact.CASKey(data)
	if key != ref.Key || *ref.Integrity != integrity {
		return nil, contentMeta{}, &artifact.Error{Code: artifact.ErrCorrupt, Operation: op, Identity: string(ref.Key), Detail: "stored bytes do not match the ref's integrity"}
	}
	if ref.SizeBytes != nil && *ref.SizeBytes != uint64(len(data)) {
		return nil, contentMeta{}, &artifact.Error{Code: artifact.ErrCorrupt, Operation: op, Identity: string(ref.Key), Detail: "stored size does not match the ref"}
	}
	if ref.MediaType != meta.MediaType {
		return nil, contentMeta{}, &artifact.Error{Code: artifact.ErrCorrupt, Operation: op, Identity: string(ref.Key), Detail: "ref media type does not match the stored declaration"}
	}
	return data, meta, nil
}

func (s *ContentStore) info(data []byte, meta contentMeta, durability artifact.Durability) artifact.Info {
	size := uint64(len(data))
	_, integrity := artifact.CASKey(data)
	return artifact.Info{MediaType: meta.MediaType, SizeBytes: &size, Integrity: &integrity, Durability: durability}
}

func (s *ContentStore) Stat(ctx context.Context, ref artifact.Ref) (artifact.Info, error) {
	if err := ctx.Err(); err != nil {
		return artifact.Info{}, err
	}
	data, meta, err := s.locate("stat", ref)
	if err != nil {
		return artifact.Info{}, err
	}
	return s.info(data, meta, ref.Durability), nil
}

func (s *ContentStore) Open(ctx context.Context, ref artifact.Ref) (io.ReadCloser, artifact.Info, error) {
	if err := ctx.Err(); err != nil {
		return nil, artifact.Info{}, err
	}
	data, meta, err := s.locate("open", ref)
	if err != nil {
		return nil, artifact.Info{}, err
	}
	return io.NopCloser(bytes.NewReader(data)), s.info(data, meta, ref.Durability), nil
}

func (s *ContentStore) Promote(ctx context.Context, ref artifact.Ref, req artifact.PromoteRequest) (artifact.Ref, error) {
	if err := ctx.Err(); err != nil {
		return artifact.Ref{}, err
	}
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
	data, meta, err := s.locate(opPromote, ref)
	if err != nil {
		return artifact.Ref{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.Durability.Rank() > meta.Durability.Rank() {
		meta.Durability = req.Durability
		if err := s.writeMeta(ref.Key, meta); err != nil {
			return artifact.Ref{}, err
		}
	}
	return s.refFor(ref.Key, data, meta, req.Durability), nil
}

var _ artifact.ContentStore = (*ContentStore)(nil)
