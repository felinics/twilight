package writer

import (
	"context"
	"errors"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/session"
)

// Delete drops a Session's root (SES-GC-1). It touches no claim: a commit's
// retention claim belongs to the segment that holds the commit (EXT-WRT-5),
// and the segment lives for as long as any root reaches it, so the content
// a fork inherits stays retained through the same claims that always
// retained it. Collect releases claims together with the segments and
// commits it reclaims (SES-GC-3). The Session must not be open in this
// process; close its Writer first. A root already deleted is not an error.
func Delete(ctx context.Context, store session.Store, sid session.SessionID) error {
	if store == nil {
		return errors.New("writer: nil store")
	}
	if err := store.Delete(ctx, sid); err != nil && !session.IsCode(err, session.ErrNotFound) {
		return err
	}
	return nil
}

// Collect reclaims the commit segments and suffixes no live Session reaches
// (SES-GC-2) and then releases the commit claims of what it reclaimed
// (SES-GC-3): every claim owned by a removed segment, and the claims of the
// commits a truncation dropped. Storage goes first and claims second, so a
// failure between the two leaks a claim rather than freeing content a
// commit still names; a repeated Collect cannot see the removed segment
// again, so the leak is repaired by releasing that segment's scope by hand.
func Collect(ctx context.Context, store session.Store, admission Admission) (session.CollectReport, error) {
	if store == nil {
		return session.CollectReport{}, errors.New("writer: nil store")
	}
	report, err := store.Collect(ctx)
	if err != nil || admission.Ledger == nil {
		return report, err
	}
	for _, seg := range report.Removed {
		if err := releaseOwned(ctx, admission.Ledger, string(seg), nil); err != nil {
			return report, err
		}
	}
	for seg, ids := range report.Dropped {
		identities := make([]string, len(ids))
		for i, id := range ids {
			identities[i] = string(id)
		}
		if err := releaseOwned(ctx, admission.Ledger, string(seg), identities); err != nil {
			return report, err
		}
	}
	return report, nil
}

// releaseOwned releases every Active commit claim of one segment, or of the
// listed commits of it when identities is non-nil.
func releaseOwned(ctx context.Context, ledger artifact.RetentionLedger, segment string, identities []string) error {
	var cursor artifact.ClaimCursor
	for {
		page, err := ledger.ClaimsByOwner(ctx, artifact.ClaimOwnerQuery{Kind: ClaimOwnerKind, Authority: segment, Identities: identities}, cursor)
		if err != nil {
			return err
		}
		for i := range page.Items {
			if page.Items[i].State != artifact.ClaimActive {
				continue
			}
			if err := ledger.ReleaseActive(ctx, page.Items[i].ID); err != nil {
				return err
			}
		}
		if page.Next == nil {
			return nil
		}
		cursor = *page.Next
	}
}
