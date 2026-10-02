// Package session is the Session kernel: the lineage tree of immutable
// commit segments and the storage, ownership and read machinery over them.
// The commit vocabulary is the ledger's and the module framework of codecs,
// stream declarations and projections is the module package's; this package
// owns what names a Session: segments and their parent edges, roots and
// their paths, writer ownership with epoch fencing, ordered reads over an
// append-only store, and the projection read path over the Store.
package session

import (
	"context"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
)

type SessionID string

// Session is one live root and its loaded path. Reads and fork-point
// resolution go through it. Appending is a Handle, which keeps the lease and
// the tip head on top of a Session. The value is tied to the Storage it was
// loaded from. A stored path is loaded by reading each span's segment; a root
// written before paths existed is assembled by walking parent edges.
type Session struct {
	root SessionRecord
	path *LoadedPath
	st   Storage
}

// ID is the Session's root identity.
func (s *Session) ID() SessionID { return s.root.ID }

// Record is the root: the tip segment, the stored path and the creation time.
func (s *Session) Record() SessionRecord { return s.root }

// Header is the tip segment's creation record.
func (s *Session) Header() SegmentHeader { return s.path.Header() }

// Tip is the segment the Session appends to.
func (s *Session) Tip() Segment { return s.path.Tip() }

// Loaded is the path from the root segment to the tip, with each segment read.
func (s *Session) Loaded() *LoadedPath { return s.path }

// Load reads a live Session's root and the loaded path of its tip.
func (l *Ledger) Load(ctx context.Context, sid SessionID) (*Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root, err := l.st.Record(ctx, sid)
	if err != nil {
		return nil, err
	}
	path, err := l.materialize(ctx, root)
	if err != nil {
		return nil, err
	}
	return &Session{root: root, path: path, st: l.st}, nil
}

// materialize turns the root's stored path into a loaded path. An empty path
// is a root written before paths were stored; its spans come from parent edges.
func (l *Ledger) materialize(ctx context.Context, root SessionRecord) (*LoadedPath, error) {
	if len(root.Path) == 0 {
		return loadPathFromEdges(ctx, l.st, root.Tip)
	}
	if err := root.Path.Validate(root.Tip); err != nil {
		return nil, newError(ErrCorrupt, "load", root.ID, err.Error())
	}
	a := &LoadedPath{Segments: make([]LoadedSpan, len(root.Path))}
	for i, span := range root.Path {
		seg, err := l.st.Segment(ctx, span.Segment)
		if err != nil {
			return nil, err
		}
		a.Segments[i] = LoadedSpan{Segment: seg, From: span.From, End: span.End}
	}
	return a, nil
}

// sessionPath is the root's stored path, or the path assembled from parent
// edges when the root has none.
func (l *Ledger) sessionPath(ctx context.Context, root SessionRecord) (Path, error) {
	if len(root.Path) == 0 {
		anc, err := loadPathFromEdges(ctx, l.st, root.Tip)
		if err != nil {
			return nil, err
		}
		return pathFromLoaded(anc), nil
	}
	if err := root.Path.Validate(root.Tip); err != nil {
		return nil, newError(ErrCorrupt, "load", root.ID, err.Error())
	}
	return root.Path, nil
}

// tipSegment reads the tip's creation record without walking its parents.
// Header and Create's idempotency check need nothing else.
func (l *Ledger) tipSegment(ctx context.Context, sid SessionID) (Segment, error) {
	root, err := l.st.Record(ctx, sid)
	if err != nil {
		return Segment{}, err
	}
	return l.st.Segment(ctx, root.Tip)
}

// ReadCommits returns the stitched commits of the Session from from,
// inclusive, at most limit (0 = unlimited).
func (s *Session) ReadCommits(ctx context.Context, from ledger.CommitSeq, limit uint32) (CommitPage, error) {
	commits, head, more, err := s.path.Read(ctx, s.st, from, limit)
	if err != nil {
		return CommitPage{}, err
	}
	return CommitPage{Header: s.Header(), Commits: commits, Head: head, HasMore: more}, nil
}

// ReadStream returns the events of one logical stream in the order the
// requested lineage sees them.
func (s *Session) ReadStream(ctx context.Context, stream ledger.Domain, lineage module.Inheritance, from ledger.StreamSeq, limit uint32) (StreamPage, error) {
	if err := validateStreamRead(s.ID(), stream, lineage); err != nil {
		return StreamPage{}, err
	}
	return s.collectStream(ctx, stream, lineage, from, limit)
}

func (s *Session) collectStream(ctx context.Context, stream ledger.Domain, lineage module.Inheritance, from ledger.StreamSeq, limit uint32) (StreamPage, error) {
	commits, err := s.path.ReadStream(ctx, s.st, stream, lineage)
	if err != nil {
		return StreamPage{}, err
	}
	head, err := s.path.tipHead(ctx, s.st)
	if err != nil {
		return StreamPage{}, err
	}
	page := StreamPage{Header: s.Header(), Domain: stream, Head: head}
	page.Events, page.HasMore = StreamEvents(commits, stream, from, limit)
	return page, nil
}

func validateStreamRead(sid SessionID, stream ledger.Domain, lineage module.Inheritance) error {
	if err := ledger.ValidateStreamRef(stream); err != nil {
		return newError(ErrInvalid, "read_stream", sid, err.Error())
	}
	if err := module.ValidateInheritance(lineage); err != nil {
		return newError(ErrInvalid, "read_stream", sid, err.Error())
	}
	return nil
}

// EdgeAt resolves a stitched CommitSeq to the parent edge a fork records:
// the segment that contributes that commit, and the seq within it. ok is
// false when the Session has no such commit. The segment must still hold it
// (SES-FRK-1).
func (s *Session) EdgeAt(ctx context.Context, seq ledger.CommitSeq) (CommitRef, bool, error) {
	span, ok := s.path.At(seq)
	if !ok {
		return CommitRef{}, false, nil
	}
	commits, _, _, err := s.st.ReadSegment(ctx, span.Segment.ID(), seq, 1)
	if err != nil {
		return CommitRef{}, false, err
	}
	if len(commits) != 1 || commits[0].Seq != seq {
		return CommitRef{}, false, nil
	}
	return CommitRef{Segment: span.Segment.ID(), Seq: seq}, true, nil
}

// repairTip checks the tip segment's CommitIndex against its head and
// rebuilds the index when it lags (SES-REP-5). Acquire has already repaired
// a torn tail, so the head this returns is the head a new Handle starts from.
func (s *Session) repairTip(ctx context.Context) (ledger.Head, error) {
	tip := s.Tip()
	summary, head, err := s.st.Summarize(ctx, tip.ID())
	if err != nil {
		return ledger.Head{}, err
	}
	if summary.Valid(tip.Seed(), head) {
		return head, nil
	}
	commits, head, _, err := s.st.ReadSegment(ctx, tip.ID(), tip.Seed().Next, 0)
	if err != nil {
		return ledger.Head{}, err
	}
	if err := s.st.PutIndex(ctx, tip.ID(), BuildCommitIndex(tip.Header, commits)); err != nil {
		return ledger.Head{}, err
	}
	return head, nil
}
