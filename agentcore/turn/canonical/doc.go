// Package canonical is the canonical identity of a Turn: the digest rules
// (Digests) for every body a fact names or a derived identity covers, and the
// derivation (Identity) of every TurnID-scoped identity -- step, call,
// response, effect and command ids. Both are persisted
// semantics: their preimages never change. The state machine of
// agentcore/turn is composed with them in agentcore/turn/schema. The package
// also declares the envelope types under which agentcore/turn/frozen stores
// the bodies these digests name.
package canonical
