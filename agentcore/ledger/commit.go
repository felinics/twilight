package ledger

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/jsonstable"
)

// Commit is one atomic step of a ledger: the batches of one operation under
// one CommitID. The commit may span several domains; the store either lands
// every batch or none. A ledger without domains writes every commit as one
// batch of the empty domain. Seq is the commit's position, assigned by the
// store; the store never rewrites or removes a commit, so the two name it
// for good.
type Commit struct {
	Seq      CommitSeq    `json:"seq"`
	CommitID CommitID     `json:"commitId"`
	Batches  []EventBatch `json:"batches"`
}

// Proposal is one atomic append: the batches of a single commit under one
// CommitID, before the store assigns Seq. The commit may span several
// domains; the store lands every batch or none.
type Proposal struct {
	CommitID CommitID
	Batches  []EventBatch
}

// At returns the proposal as a commit positioned at seq. It does not
// validate; callers validate the proposal before assigning a position.
func (p Proposal) At(seq CommitSeq) Commit {
	return Commit{Seq: seq, CommitID: p.CommitID, Batches: p.Batches}
}

// Validate checks the shape of a proposal: a valid CommitID and well-formed
// batches. Seq is not part of a proposal.
func (p Proposal) Validate() error {
	c := p.At(0)
	return c.Validate()
}

// ValidateEvent checks one event before it is stored.
func ValidateEvent(e Event) error {
	return validateEventShape(e.Type, e.Payload)
}

// ValidateBatches checks the shape of one proposed commit before it is
// stored: non-empty batches, unique domains within the commit, valid
// attribution and canonical payloads. Duplicate CommitIDs and epoch fencing
// are store duties.
func ValidateBatches(batches []EventBatch) error {
	if len(batches) == 0 {
		return errors.New("commit without batches")
	}
	seen := make(map[Domain]struct{}, len(batches))
	for i := range batches {
		b := &batches[i]
		if err := ValidateDomain(b.Domain); err != nil {
			return fmt.Errorf("batch %d: %w", i, err)
		}
		if _, dup := seen[b.Domain]; dup {
			return fmt.Errorf("batch %d: domain %s appears twice in one commit", i, b.Domain)
		}
		seen[b.Domain] = struct{}{}
		if len(b.Events) == 0 {
			return fmt.Errorf("batch %d: no events", i)
		}
		for j := range b.Events {
			if err := ValidateEvent(b.Events[j]); err != nil {
				return fmt.Errorf("batch %d event %d: %w", i, j, err)
			}
		}
	}
	return nil
}

// Validate checks the shape of a commit before it is stored: a valid
// CommitID and well-formed batches. Seq and duplicate CommitIDs are the
// store's checks.
func (c *Commit) Validate() error {
	if err := ValidIdentity("CommitID", string(c.CommitID)); err != nil {
		return err
	}
	return ValidateBatches(c.Batches)
}

// validateEventShape checks the event invariants: a non-empty valid-UTF-8
// type and a canonical JSON object payload.
func validateEventShape(typ EventType, payload jsonstable.Value) error {
	if err := ValidIdentity("EventType", string(typ)); err != nil {
		return err
	}
	if payload.IsZero() {
		return errors.New("empty payload")
	}
	canon, err := jsonstable.Canonicalize(payload.Bytes())
	if err != nil {
		return fmt.Errorf("payload: %w", err)
	}
	if !bytes.Equal(canon, payload.Bytes()) {
		return errors.New("payload is not canonical")
	}
	if !bytes.HasPrefix(bytes.TrimSpace(payload.Bytes()), []byte("{")) {
		return errors.New("payload is not an object")
	}
	return nil
}
