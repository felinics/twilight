// Package decision is the decision seam of the agent core
// (docs/design/agent-decision.md): the catalog that resolves an AgentPreset's
// PromptBuilderRef to a PromptBuilder and the Sources a builder reads from.
// It holds no builder of its own and no input content shape (the agent's
// input package owns that, DEC-INP-1): how a model
// request is assembled from Session state is a strategy of the agent built on
// the core (agent/prompt is the first-party one), named by a ref the
// AgentPreset digest covers, so the process that takes a Turn over resolves
// the same function from the same catalog.
package decision

import (
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/session/extension"
)

// ProjectionSource is what a prompt builder reads state from: the owner
// process serves it from the Session Writer (writer.Projections), an observer
// from the Store.
type ProjectionSource = extension.ProjectionReader

// Sources are the two read ports of a prompt builder (DEC-PMT-1): the
// structural projections and the content resolver that materializes the
// frozen bodies the projections name by digest. Folding is pure; reading a
// body is I/O and happens only here.
type Sources struct {
	Projections ProjectionSource
	Content     chatlog.ContentResolver
}
