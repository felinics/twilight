// Package canonical is the canonical identity of a Run: the digest rules
// (Digests) for every body a fact names or a derived identity covers, and the
// derivation (Identity) of every RunID-scoped identity -- step, call,
// response, effect and command ids (RUN-WIR-1, RUN-WIR-4). Both are persisted
// semantics: their preimages never change (RUN-CMT-8). The state machine of
// agentcore/run is composed with them in agentcore/run/schema. The package
// also declares the envelope types under which agentcore/run/frozen stores
// the bodies these digests name.
package canonical
