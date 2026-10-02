// Package lifecycle creates, forks and reclaims Sessions over one Store; it
// knows no module's facts, so a fork's preconditions over history are the
// caller's.
package lifecycle

import (
	"context"
	"errors"
	"time"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
)

// Lifecycle creates, forks and reclaims Sessions over one Store. Every
// method changes the set of Sessions or their storage; none opens one.
type Lifecycle struct {
	Store     session.Stores
	Registry  *module.Registry
	Admission writer.Admission
	Clock     func() time.Time
}

func (l Lifecycle) now() int64 {
	if l.Clock != nil {
		return l.Clock().UnixMilli()
	}
	return time.Now().UnixMilli()
}

// Create creates the Session; ext are the segment's module extension slots
// (nil for none), carried opaquely (SES-WIR-5).
func (l Lifecycle) Create(ctx context.Context, sid session.SessionID, ext module.Extensions) error {
	_, err := l.Store.Create(ctx, session.CreateRequest{SessionID: sid, CreatedAtUnixMilli: l.now(), Ext: ext})
	return err
}

// Ensure makes sure the Session exists, whatever record created it: a root
// made here, a fork or a spawned child all count. Create alone would refuse
// a Session whose segment fields differ (SES-CRT-1), so existence is probed
// first.
func (l Lifecycle) Ensure(ctx context.Context, sid session.SessionID) error {
	if _, err := l.Store.Header(ctx, sid); err == nil {
		return nil
	} else if !session.IsNotFound(err) {
		return err
	}
	if err := l.Create(ctx, sid, nil); err != nil {
		// A concurrent creator winning the race is still "exists".
		if _, herr := l.Store.Header(ctx, sid); herr == nil {
			return nil
		}
		return err
	}
	return nil
}

// ForkRequest forks a Session at one commit of its ledger (OWN-FRK-1): the
// child inherits every commit of Parent up to and including At and continues
// from there under its own identity.
type ForkRequest struct {
	Parent session.SessionID
	At     ledger.CommitSeq
	Child  session.SessionID
	// Ext are the child segment's module extension slots (SES-WIR-5).
	Ext module.Extensions
}

// Fork creates the child Session (SES-FRK-1) and claims the artifacts its
// inherited prefix references (EXT-WRT-8). The child is not opened.
func (l Lifecycle) Fork(ctx context.Context, req ForkRequest) (session.SegmentHeader, error) {
	if req.Parent == "" || req.Child == "" {
		return session.SegmentHeader{}, errors.New("lifecycle: fork requires parent and child session ids")
	}
	if req.Parent == req.Child {
		return session.SegmentHeader{}, errors.New("lifecycle: a session cannot fork itself")
	}
	return writer.Fork(ctx, l.Store, l.Registry, writer.ForkRequest{
		Parent: req.Parent, At: req.At, Child: req.Child, CreatedAtUnixMilli: l.now(), Ext: req.Ext,
	})
}

// Delete tombstones the Session and reclaims along its path, releasing the
// retention claims of what it reclaimed (SES-GC-1/2/3). The Session must not
// be open in this process.
func (l Lifecycle) Delete(ctx context.Context, sid session.SessionID) error {
	return writer.Delete(ctx, l.Store, l.Admission, sid)
}

// Collect reclaims the storage of deleted Sessions no live Session reaches
// (SES-GC-2) and releases the claims of the commits it reclaimed (SES-GC-3).
func (l Lifecycle) Collect(ctx context.Context) (session.CollectReport, error) {
	return writer.Collect(ctx, l.Store, l.Admission)
}
