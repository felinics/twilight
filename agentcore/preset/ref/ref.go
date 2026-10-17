// Package ref defines immutable preset identities without depending on the
// agent state machine or the preset catalog.
package ref

import "github.com/felinics/twilight/agentcore/jsonstable"

type ID string

// Ref names the registered preset and the exact decision configuration.
type Ref struct {
	ID     ID                `json:"id"`
	Digest jsonstable.Digest `json:"digest"`
}
