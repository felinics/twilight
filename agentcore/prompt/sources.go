// Package prompt is the prompt-building seam of the agent core: the Builder
// that assembles the next model input, the Catalog that resolves an
// AgentPreset's PromptBuilderRef to one, and the Sources a builder reads
// from. It holds no builder of its own and no input content shape: how a
// model request is assembled from Session state is a strategy of the agent
// built on the core, named by a ref the AgentPreset digest covers, so the
// process that takes a Turn over resolves the same function from the same
// catalog.
package prompt

import (
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/session"
)

// ProjectionSource is what a prompt builder reads state from: the owner
// process serves it from the Session Writer (writer.Projections), an observer
// from the Store.
type ProjectionSource = session.ProjectionReader

// Sources are the two read ports of a prompt builder: the structural
// projections and the content resolver that materializes the frozen bodies
// the projections name by digest. Folding is pure; reading a body is I/O
// and happens only here.
type Sources struct {
	Projections ProjectionSource
	Content     chatlog.ContentResolver
}
