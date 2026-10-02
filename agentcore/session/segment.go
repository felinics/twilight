package session

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
)

// SegmentID identifies one commit segment independently of any host
// (SES-WIR-4).
type SegmentID string

// CommitRef names one commit in a lineage tree: the commit at Seq of a
// segment, by its place in the stitched sequence. As SegmentHeader.Parent it
// is the edge from a child segment to the last commit it inherits: the
// child's own commits are numbered from Seq+1 and readers see the prefix
// [0, Seq] followed by them. History is append-only, so (Segment, Seq)
// names one commit for good and the edge is a stable reference (SES-FRK-1).
type CommitRef struct {
	Segment SegmentID        `json:"segment"`
	Seq     ledger.CommitSeq `json:"seq"`
}

// Validate checks the shape of a parent edge. A nil edge is a root segment;
// otherwise it names a parent segment. Whether the parent still holds the
// commit is the store's check.
func (edge *CommitRef) Validate() error {
	if edge == nil {
		return nil
	}
	return ledger.ValidIdentity("Parent.Segment", string(edge.Segment))
}

// SegmentHeader is the immutable creation record of a commit segment. It
// names no host: which roots append to or include the segment is the
// roots' business.
type SegmentHeader struct {
	// ID is the segment's identity, drawn at random: two segments with
	// equal records are still two nodes (SES-WIR-4).
	ID          SegmentID          `json:"id"`
	Parent      *CommitRef         `json:"parent,omitempty"` // nil for a root segment; the edge to the parent otherwise
	CausationID ledger.CausationID `json:"causationId,omitempty"`
	// Ext are the module extension slots of the creation record, opaque to
	// readers that know no module (SES-WIR-5).
	Ext module.Extensions `json:"ext,omitempty"`
}

// Segment is one node of a lineage forest: a creation record. Its
// commits live in the store, numbered from Header.Seed().
type Segment struct {
	Header SegmentHeader
}

// ID is the segment's identity.
func (s Segment) ID() SegmentID { return s.Header.ID }

// NewSegmentID returns a fresh segment identity: 128 random bits, hex encoded.
func NewSegmentID() (SegmentID, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("ledger: segment id: %w", err)
	}
	return SegmentID(hex.EncodeToString(b[:])), nil
}

// Parent returns the edge to the parent segment, or nil for a root.
func (s Segment) Parent() *CommitRef {
	if s.Header.Parent == nil {
		return nil
	}
	edge := *s.Header.Parent
	return &edge
}

// Seed is the head of the segment while it holds no commits of its own.
func (s Segment) Seed() ledger.Head { return s.Header.Seed() }

// Seed is the head of a segment that holds no commits of its own: a root
// segment starts at 0, a child continues its parent's numbering at
// Parent.Seq+1 (SES-FRK-2).
func (h SegmentHeader) Seed() ledger.Head {
	if h.Parent != nil {
		return ledger.Head{Next: h.Parent.Seq + 1}
	}
	return ledger.Head{}
}

// Validate checks the shape of a segment's creation record: a non-empty ID,
// a well-formed edge and a well-formed Ext.
func (h SegmentHeader) Validate() error {
	if err := ledger.ValidIdentity("segment ID", string(h.ID)); err != nil {
		return fmt.Errorf("segment header: %w", err)
	}
	if err := h.Parent.Validate(); err != nil {
		return fmt.Errorf("segment header: %w", err)
	}
	return module.ValidateExtensions(h.Ext)
}
