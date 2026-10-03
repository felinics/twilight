package writer

import (
	"context"
	"errors"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
)

// ForkRequest creates a child Session from a parent's ledger prefix.
type ForkRequest struct {
	Parent session.SessionID
	// At is the last parent commit the child inherits.
	At                 ledger.CommitSeq
	Child              session.SessionID
	CreatedAtUnixMilli int64
	// Ext are the child segment's module extension slots.
	Ext module.Extensions
}

// Fork creates req.Child from req.Parent's history at commit req.At
// . It holds no claim of its
// own (EXT-WRT-8): the content the inherited prefix references is retained
// by the commit claims of the segments that hold those commits. Those
// commits stay while a live path's span still covers them.
// Fork is idempotent: a repeat with the same arguments returns the same
// header.
func Fork(ctx context.Context, store session.Store, registry *module.Registry, req ForkRequest) (session.SegmentHeader, error) {
	if store == nil || registry == nil {
		return session.SegmentHeader{}, errors.New("writer: nil store or registry")
	}
	if req.Parent == "" || req.Child == "" {
		return session.SegmentHeader{}, errors.New("writer: fork requires parent and child session ids")
	}
	return store.Create(ctx, session.CreateRequest{SessionID: req.Child,
		CreatedAtUnixMilli: req.CreatedAtUnixMilli, Fork: &session.ForkOrigin{Session: req.Parent, Seq: req.At}, Ext: req.Ext})
}
