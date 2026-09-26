// Package run defines a Run: one execution attempt with a unique identity that
// is recoverable and never rewritten. It holds the Run's identifiers and value
// types, the MachineState, the sealed command and fact variants, and the legal
// state transitions (Decide, Evolve, CreateGroup) of each schema version.
//
// A Run is a logical execution history. Its vocabulary keeps four things
// apart: a Step (ModelStep, ToolStep) is logical progress; an effect
// (EffectID) is one request the Run makes of the outside world, the model
// call of a step or one tool call; an attempt is one physical execution of
// an effect and belongs to the executor, which the Run reaches only through
// the EffectID; a Wait (ResponseRequest) is the external input a tool call
// lacks. The Run records that an effect was requested, settled or recovered
// as unknown, never who executed it or how.
//
// The Machine computes no digest and derives no identity itself: it is bound
// to the Canonical and Identity rules of the schema version a Run was created
// under, so a Run replays under its own rules whatever the current version is.
//
// Everything around a Run lives in the subpackages, in two tiers (RUN-SCP-1).
// The protocol tier changes with the Run schema or is a port the core defines:
// agent/run/model holds the durable model data that facts name by digest,
// agent/run/canonical the digest and identity rules, agent/run/wire the
// persisted fact and command protocol, agent/run/frozen the frozen-body
// protocol, agent/run/schema the version binding, agent/run/plan the next
// action and the takeover dispositions, and agent/run/runtime the RunStore
// port and commit evaluation. Outside model/sdkconv the protocol tier depends
// only on agent/es, agent/jsonstable and itself.
//
// The execution tier uses the protocol tier and the sdk, and neither the core
// nor the protocol tier imports it: agent/run/effect is the process-independent
// port between the Loop and the Executor, agent/run/loop the in-process
// execution interpreter with its model/tool ports, and agent/run/reconcile the
// takeover reconciliation against the execution store. The transport encoding
// of the effect port belongs to the Executor, in agent/executor/protocol.
//
// See docs/design/agent-run.md for the governing specification.
package run
