package session

import (
	"context"
	"fmt"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
)

// Span is one segment's contribution on a session path. From is the first
// stitched CommitSeq it contributes. End is closed for every span except
// the last, which stays open: that span is the session's only append target.
type Span struct {
	Segment SegmentID        `json:"segment"`
	From    ledger.CommitSeq `json:"from"`
	End     Bound            `json:"end"`
}

// Path is one session's history: a strictly increasing sequence of spans.
// The same segment appears at most once. Adjacent spans meet, so the next
// span starts at the previous span's last commit plus one. Appends do not
// add spans.
type Path []Span

// Validate checks the path shape against tip: the last span names tip and
// is open, earlier spans are closed, segments are unique, and each span
// starts where the previous one stopped.
func (p Path) Validate(tip SegmentID) error {
	if len(p) == 0 {
		return fmt.Errorf("empty path")
	}
	seen := make(map[SegmentID]struct{}, len(p))
	for i, span := range p {
		if span.Segment == "" {
			return fmt.Errorf("span %d: empty segment", i)
		}
		if _, dup := seen[span.Segment]; dup {
			return fmt.Errorf("span %d: segment %s repeated", i, span.Segment)
		}
		seen[span.Segment] = struct{}{}
		last := i == len(p)-1
		if last {
			if !span.End.Open {
				return fmt.Errorf("span %d: tip coverage is closed", i)
			}
			if span.Segment != tip {
				return fmt.Errorf("span %d: segment %s, tip %s", i, span.Segment, tip)
			}
		} else if span.End.Open || span.End.Through < span.From {
			return fmt.Errorf("span %d: closed coverage [%d, %d] open=%v", i, span.From, span.End.Through, span.End.Open)
		}
		if i == 0 {
			if span.From != 0 {
				return fmt.Errorf("span 0 starts at %d", span.From)
			}
			continue
		}
		prev := p[i-1] //nolint:gosec // G602: i > 0 here, the first span returned above
		if span.From != prev.End.Through+1 {
			return fmt.Errorf("span %d starts at %d, previous coverage ends at %d", i, span.From, prev.End.Through)
		}
	}
	return nil
}

// Branch copies the prefix of p through the span that contains seq, closes
// that span at seq, and appends an open span for next. Spans after that one
// are dropped. p is unchanged. The returned edge is the parent pointer of next.
func (p Path) Branch(seq ledger.CommitSeq, next SegmentID) (Path, CommitRef, error) {
	if len(p) == 0 {
		return nil, CommitRef{}, fmt.Errorf("empty path")
	}
	for i := range p {
		span := p[i]
		if seq < span.From || !span.End.Includes(seq) {
			continue
		}
		out := make(Path, i+2)
		copy(out, p[:i])
		out[i] = Span{Segment: span.Segment, From: span.From, End: ThroughBound(seq)}
		out[i+1] = Span{Segment: next, From: seq + 1, End: OpenBound()}
		return out, CommitRef{Segment: span.Segment, Seq: seq}, nil
	}
	return nil, CommitRef{}, fmt.Errorf("commit %d is outside the path", seq)
}

// MaxBound is the retention bound of the ends that still name a segment:
// an open end retains the whole segment, otherwise the greatest Through.
// ok is false when ends is empty.
func MaxBound(ends []Bound) (Bound, bool) {
	if len(ends) == 0 {
		return Bound{}, false
	}
	widest := ends[0]
	for _, end := range ends[1:] {
		widest = mergeBound(widest, end)
	}
	return widest, true
}

func mergeBound(a, b Bound) Bound {
	if a.Open || b.Open {
		return OpenBound()
	}
	if b.Through > a.Through {
		return ThroughBound(b.Through)
	}
	return a
}

func pathFromLoaded(a *LoadedPath) Path {
	if a == nil || len(a.Segments) == 0 {
		return nil
	}
	out := make(Path, len(a.Segments))
	for i := range a.Segments {
		s := &a.Segments[i]
		out[i] = Span{Segment: s.Segment.ID(), From: s.From, End: s.End}
	}
	return out
}

// Bound is how much of a segment a path contributes or a root retains.
// Open covers every commit the segment has, through its live head; otherwise
// Through is the last included CommitSeq.
type Bound struct {
	Open    bool             `json:"open,omitempty"`
	Through ledger.CommitSeq `json:"through,omitempty"`
}

// OpenBound retains or contributes every commit of a segment.
func OpenBound() Bound { return Bound{Open: true} }

// ThroughBound retains or contributes commits through seq, inclusive.
func ThroughBound(seq ledger.CommitSeq) Bound { return Bound{Through: seq} }

// Includes reports whether seq is inside the bound. An open bound
// includes every seq.
func (c Bound) Includes(seq ledger.CommitSeq) bool {
	return c.Open || seq <= c.Through
}

// LoadedPath is a session path with each span's segment loaded. Reads and
// fork-point resolution use it. A stored path does not walk parent edges.
type LoadedPath struct {
	// Segments are ordered root first; the last is the tip a Session
	// appends to.
	Segments []LoadedSpan
}

// LoadedSpan is one span of a loaded path: the segment and the stitched
// range it contributes. A closed end stops at End.Through. The tip's end
// is open and contributes through its live head.
type LoadedSpan struct {
	Segment Segment
	// From is the first stitched CommitSeq the segment contributes.
	From ledger.CommitSeq
	// End is the last stitched CommitSeq it contributes. The tip's End is
	// open: it contributes through its live head.
	End Bound
}

// Tip is the segment the Session appends to.
func (a *LoadedPath) Tip() Segment { return a.Segments[len(a.Segments)-1].Segment }

// Header is the tip's creation record: the Session's public header.
func (a *LoadedPath) Header() SegmentHeader { return a.Tip().Header }

// At returns the loaded span that contributes seq.
func (a *LoadedPath) At(seq ledger.CommitSeq) (LoadedSpan, bool) {
	for i := range a.Segments {
		if s := &a.Segments[i]; seq >= s.From && s.End.Includes(seq) {
			return *s, true
		}
	}
	return LoadedSpan{}, false
}

// loadPathFromEdges builds a loaded path by walking parent edges from tip
// to the root. It is the fallback for a root stored before paths were
// written. Each segment contributes from its seed through the child edge's
// seq; the tip stays open.
func loadPathFromEdges(ctx context.Context, store SegmentStore, tip SegmentID) (*LoadedPath, error) {
	var chain []Segment
	seen := map[SegmentID]bool{}
	for id := tip; ; {
		if seen[id] {
			return nil, &Error{Code: ErrCorrupt, Operation: "path", Detail: fmt.Sprintf("segment %s is its own ancestor", id)}
		}
		seen[id] = true
		seg, err := store.Segment(ctx, id)
		if err != nil {
			return nil, err
		}
		chain = append(chain, seg)
		parent := seg.Parent()
		if parent == nil {
			break
		}
		id = parent.Segment
	}
	a := &LoadedPath{Segments: make([]LoadedSpan, len(chain))}
	for i := range chain {
		seg := chain[len(chain)-1-i] // root first
		s := LoadedSpan{Segment: seg, From: seg.Seed().Next, End: OpenBound()}
		if i+1 < len(chain) {
			s.End = ThroughBound(chain[len(chain)-2-i].Parent().Seq)
		}
		a.Segments[i] = s
	}
	return a, nil
}

// Read returns the stitched commits of the loaded path from from (inclusive),
// at most limit (0 = unlimited), the tip's head and whether more follow. It
// iterates the explicit path; each segment is read once for its range.
func (a *LoadedPath) Read(ctx context.Context, store SegmentStore, from ledger.CommitSeq, limit uint32) ([]ledger.Commit, ledger.Head, bool, error) {
	var out []ledger.Commit
	var head ledger.Head
	tip := len(a.Segments) - 1
	for i := range a.Segments {
		s := &a.Segments[i]
		start := from
		if start < s.From {
			start = s.From
		}
		if !s.End.Open && start > s.End.Through {
			continue
		}
		var want uint32
		if limit > 0 {
			remaining := int(limit) - len(out)
			if remaining <= 0 {
				head, err := a.tipHead(ctx, store)
				return out, head, true, err
			}
			want = ledger.Limit32(uint64(remaining))
			if !s.End.Open && ledger.CommitSeq(want) > s.End.Through-start+1 {
				want = ledger.Limit32(uint64(s.End.Through - start + 1))
			}
		} else if !s.End.Open {
			want = ledger.Limit32(uint64(s.End.Through - start + 1))
		}
		commits, segHead, more, err := store.ReadSegment(ctx, s.Segment.ID(), start, want)
		if err != nil {
			return nil, ledger.Head{}, false, err
		}
		if s.End.Open {
			head = segHead
			out = append(out, commits...)
			return out, head, more, nil
		}
		for _, c := range commits {
			if !s.End.Includes(c.Seq) {
				break
			}
			out = append(out, c)
		}
		if AtLimit(len(out), limit) {
			// Anything after the last returned commit exists by construction:
			// either the rest of this segment's range or the segments below.
			last := out[len(out)-1].Seq
			head, err := a.tipHead(ctx, store)
			return out, head, last < s.End.Through || i < tip, err
		}
	}
	return out, head, false, nil
}

// ReadStream returns, in stitched order, the commits of the loaded path that
// carry a batch of stream: every segment's under Inherited, the tip's own
// under Own. Each segment is read through
// SegmentStore.ReadSegmentStream within the range it contributes.
func (a *LoadedPath) ReadStream(ctx context.Context, store SegmentStore, stream ledger.Domain, lineage module.Inheritance) ([]ledger.Commit, error) {
	var out []ledger.Commit
	tip := len(a.Segments) - 1
	first := 0
	if lineage == module.Own {
		first = tip
	}
	for i := first; i <= tip; i++ {
		s := &a.Segments[i]
		commits, _, err := store.ReadSegmentStream(ctx, s.Segment.ID(), stream, s.From, 0)
		if err != nil {
			return nil, err
		}
		for _, c := range commits {
			if !s.End.Open && !s.End.Includes(c.Seq) {
				break
			}
			out = append(out, c)
		}
	}
	return out, nil
}

// tipHead reads the tip's head without reading commits.
func (a *LoadedPath) tipHead(ctx context.Context, store SegmentStore) (ledger.Head, error) {
	_, head, _, err := store.ReadSegment(ctx, a.Tip().ID(), ^ledger.CommitSeq(0), 1)
	return head, err
}

// Contains reports whether id is a commit of the loaded path: one of the tip's
// own commits or an inherited one within its anchor range (SES-FRK-3). It
// consults the segments' indexes only (SES-REP-5).
func (a *LoadedPath) Contains(ctx context.Context, store SegmentStore, id ledger.CommitID) (bool, error) {
	return locateIn(ctx, store, a.Segments, id)
}

// ContainsInherited is Contains over the loaded path without its tip.
func (a *LoadedPath) ContainsInherited(ctx context.Context, store SegmentStore, id ledger.CommitID) (bool, error) {
	return locateIn(ctx, store, a.Segments[:len(a.Segments)-1], id)
}

// Lookup reads a commit of the loaded path by CommitID.
func (a *LoadedPath) Lookup(ctx context.Context, store SegmentStore, id ledger.CommitID) (ledger.Commit, bool, error) {
	return lookupIn(ctx, store, a.Segments, id)
}

// LookupInherited reads an inherited commit by CommitID: the loaded path without
// its tip. The inherited prefix is immutable, so reading it from storage at
// any time gives the same answer.
func (a *LoadedPath) LookupInherited(ctx context.Context, store SegmentStore, id ledger.CommitID) (ledger.Commit, bool, error) {
	return lookupIn(ctx, store, a.Segments[:len(a.Segments)-1], id)
}

func lookupIn(ctx context.Context, store SegmentStore, segments []LoadedSpan, id ledger.CommitID) (ledger.Commit, bool, error) {
	for i := len(segments) - 1; i >= 0; i-- {
		s := segments[i]
		c, ok, err := store.LookupCommit(ctx, s.Segment.ID(), id)
		if err != nil {
			return ledger.Commit{}, false, err
		}
		if ok && c.Seq >= s.From && s.End.Includes(c.Seq) {
			return c, true, nil
		}
	}
	return ledger.Commit{}, false, nil
}

// locateIn reports whether id is a commit of segments within their anchor
// ranges, from the segments' indexes alone.
func locateIn(ctx context.Context, store SegmentStore, segments []LoadedSpan, id ledger.CommitID) (bool, error) {
	for i := len(segments) - 1; i >= 0; i-- {
		s := &segments[i]
		seq, ok, err := store.Locate(ctx, s.Segment.ID(), id)
		if err != nil {
			return false, err
		}
		if ok && seq >= s.From && s.End.Includes(seq) {
			return true, nil
		}
	}
	return false, nil
}
