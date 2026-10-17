// Package turn defines one logical agent execution. A Turn owns an immutable
// preset reference, accepted inputs, a deterministic state machine, and its
// complete fact history. It starts once and ends once; process recovery and
// ownership changes continue the same identity.
//
// Decide and Evolve express legal state transitions under schema-bound
// identity and canonicalization rules. A Step is logical progress, an Effect
// is one external request, and a Wait is an external response a tool lacks.
// Physical execution attempts, workers, leases, and backend handles belong to
// the executor. The Turn addresses external work through EffectID.
//
// Subpackages model, canonical, wire, frozen, schema, plan, and store define
// durable values, protocols, and ports. Package effect defines the executor
// boundary; loop interprets decisions; reconcile handles execution recovery.
// Package sessionstore binds these protocols to the Session ledger and owns
// both the inherited history surface and the own-only active machine.
package turn
