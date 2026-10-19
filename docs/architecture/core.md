# Agent Core

Agent Core defines Twilight's durable execution model. It owns session history,
turn state, effect lifecycle, persistence, fencing, and recovery semantics.

## Semantic flow

An Input is committed to a Session's chatlog. A Turn accepts submitted inputs
under an immutable preset and owns the complete agent loop.

The Turn loop reads the current Turn state, derives the next command, and commits
its protocol transition. When the transition requires external work, the
execution boundary handles that work and returns an Outcome. The loop commits
the outcome and continues the Turn.

Session projections expose the resulting state without requiring a write
capability.

## CQRS and event sourcing

Agent Core uses event sourcing for durable semantic state and CQRS to separate
state changes from state reads.

### Event sourcing

Commands produce committed facts. The ledger stores those facts in order, and
module projections fold them into current state. The facts provide the durable
history; projections provide derived read models.

```text
Command → validate current state → append facts → fold projections
                                      │
                                      └── durable history
```

A restart rebuilds state from the ledger and retained content. A projection
cache improves read performance while the ledger remains the source of truth.

### CQRS

The write path uses a Writer bound to an already acquired Session handle.
It validates commands, appends commits atomically under the handle's epoch,
and applies projection updates. It neither acquires nor renews ownership;
closing a Writer flushes local state without releasing the handle.

The read path uses projections and query surfaces. Reads can inspect session,
turn and chatlog state without acquiring a write capability.

CQRS is a logical separation in Agent Core. The write and read paths may share
an implementation while retaining independent responsibilities.

## Durable state

Session state is stored as an append-only sequence of committed facts. A commit
contains an ordered event group and a commit identity. Projections fold facts
into queryable state.

The ledger enforces:

- commit ordering;
- idempotent commit identities;
- writer-epoch fencing.

Domain state machines validate legal transitions before the facts are appended.

A process reconstructs session state by reopening the store and folding the
recorded facts. Projection caches accelerate this operation and do not replace
the ledger.

Transient observations, such as executor progress, remain outside semantic
history unless their protocol commits them as facts.

## Core concepts

### Session

A Session is the durable boundary of one interaction. Its history contains the
inputs, turns, compaction operations, and other module-owned facts
associated with that interaction.

`writer.Factory.Bind` builds a Writer over an acquired Session handle.
Reads use projections and do not require ownership. A Writer commits under
its handle's epoch. Session identity, Turn state, and recorded effects remain
durable independently of any open handle.

### Input

An Input is a durable entry in the session chatlog. Its lifecycle separates
submission, acceptance, and consumption:

1. Chatlog records the content as `InputSubmitted`.
2. The Turn records `InputAccepted` with the input ID and content digest.
3. `ModelStepPrepared` records which accepted inputs the request consumes.

Start and delivery validate the submitted content and accept the inputs within
one Writer transaction. Replaying a delivery with the same IDs but different
content is a conflict. Chatlog derives its delivered status from `InputAccepted`;
there is no separate durable delivery event.

Inputs carry a Session-level delivery placement, not a target TurnID:

- `turn`: prefer the active Turn's next model request; if it has ended or none
  exists, use a subsequent Turn.
- `next_turn`: leave the active Turn alone and wait for a subsequent Turn; an
  idle Session can start one immediately.

Admission persists the content and placement under a stable InputID. Success
means accepted, not delivered or model execution completed. Reusing the ID with
other content or placement is a conflict. Routing atomically couples the Run's
input acceptance with Chatlog delivery, so an input is consumed at most once.
It does not cancel a model request or tool execution already in flight. Several
pending inputs may be delivered together, in submission order.

The Session runtime routes from these persisted facts during Advance/Resume,
including after reopening. The application accepts both in-process submissions
and Inbox `submit` commands through the same operation. Inbox resolution does
not replace the Chatlog queue; owners also check accepted inputs on open and
on their Inbox polls. Automatic discovery after a crashed owner relies on the
configured finite leases and activation scan. Explicitly closed Sessions need
another activation or Open; released leases are not expired-lease candidates.

Strict named-Turn delivery and idle-only starts remain separate low-level
primitives.

### Turn

A Turn is one logical agent execution with a stable TurnID. It owns its
immutable preset reference, accepted inputs, logical steps, effect lifecycle,
recovery state, and terminal result. Its complete history is recorded as Turn
facts and folded by the deterministic `Decide/Evolve` state machine.

A Turn starts once with `TurnStarted` and ends once with `TurnEnded`, whose
result is completed, failed, or stopped. The conversation-facing Turn surface
and the execution machine are projections of these same authoritative facts.

Process restart, Session takeover, Worker replacement, and execution recovery
continue the same Turn. They do not create another logical execution identity.
Physical attempts belong to the executor's Execution model.

Turn identity and canonicalization rules are bound to the schema version that
created the Turn. Replays use those rules.

### Step

A Step represents logical progress inside a Turn. The current implementation
uses model steps and tool steps.

A Step may produce one or more physical execution attempts when its effect is
recovered and replay is permitted.

### Effect

An Effect is a request for external work. Model calls and tool calls are
represented as effects. The owning protocol assigns each effect a stable
identity and records its lifecycle: a Turn for conversation steps, or a Session
compaction operation for a context-maintenance summary.

### Execution

An Execution is one physical attempt for an Effect. The executor persists the
selected provider and its opaque backend reference.

The Core addresses the execution through the effect assignment key. Backend
handle formats remain inside the executor and provider layers.

### Outcome

An Outcome is the terminal result observed from an Execution. The executor
reports success, failure, cancellation, or an unavailable result. The owning
protocol commits the outcome and performs its next transition.

An uncertain dispatch remains recoverable until the backend provides a reliable
observation. An interrupted request therefore keeps its settlement unresolved.

### Wait

A Wait records an external response required by a tool call. The Turn remains
active while waiting. The response is committed before the Turn continues.

### Artifact

Facts reference large or immutable bodies through artifact identities. The
content store retains those bodies, and the binding and retention stores keep
the references valid across restarts and forks.

## Turn loop

`agentcore/turn/loop` is the Turn decision interpreter. The TurnStore remains the
source of authority. Each operation loads the Turn, derives the next plan,
commits protocol transitions, and reloads state when another actor has advanced
it.

The finite protocol operations are:

- `Advance` interprets the Turn until it dispatches one or more assignments or
  reaches a waiting or finished state. It prepares and commits protocol
  transitions, then returns assignment keys before outcome retrieval.
- `Deliver` accepts one available Outcome, verifies that the named effect is
  still executing, commits the settlement, and reports the next action. A
  stale outcome is dropped.

A blocking loop drive is a synchronous composition of these same operations:
Advance, wait for Outcomes, Deliver, and repeat. It does not define another
execution protocol.
A tool step can have several pending effects, subject to its execution policy.

The Loop serializes operations for one Turn and permits different Turns to
advance concurrently. Its EventSink carries committed facts and provisional
model or tool observations. The ledger remains authoritative for committed
state.

Context cancellation interrupts the operation; it is not a Turn cancellation.
Domain cancellation is recorded as Turn facts. Reconciliation uses those
persisted dispositions to cancel the corresponding executions. Interrupted
settlement leaves the Outcome available for delivery under the same identity.

Acknowledgement requires both an effect-naming durable disposition and a
terminal Execution. A terminal Turn or dropped delivery alone is not proof
that an Outcome can be collected.

## Execution boundary

Every assignment is addressed by `(Session, OwnerKind, OwnerID, EffectID)`.
`OwnerKind` separates Turn work from compaction work, even when their IDs
coincide. Turn assignments use the TurnID as OwnerID; compaction assignments
use the compaction operation ID. StepID and CallID describe the requested work
and travel with the assignment rather than forming another owner identity.

The executor uses the following lifecycle:

```text
Loop.Advance
     │
     ├── Validate → Prepare → accepted commit → Start
     │                                      │
     │                                      ▼
     │                              asynchronous backend work
     │                                      │
     └── assignment key ◄────────── Worker / Backend record
                                            │
                              settlement notice or outcome read
                                            │
                                            ▼
                                      Loop.Deliver
```

`Validate` checks backend capability before an effect starts.

`Prepare` deterministically derives an execution reference. It performs no
external allocation. The Worker persists the reference before `Start`.

`Start` crosses the external effect boundary. The backend reports whether the
execution is active, terminal, absent, or uncertain.

`Attach` reconnects to an existing execution. A missing execution can be
replayed when the assignment's replay policy permits it. An uncertain or
orphaned execution remains under recovery control.

## Fencing and recovery

A Session Writer commits under an ownership epoch. Acquire installs a new
epoch, Renew extends the current handle, and Release ends that epoch's hold.
Append atomically checks the current hold, epoch, tip and expected head.
A superseded Writer cannot commit.

Session lease expiry permits takeover; it does not itself invalidate the
current epoch. The storage clock determines expiry. A stale Release cannot
revoke a successor's hold.

Execution recovery proceeds as follows:

1. The Worker persists the execution reference.
2. The Worker starts or attaches to the backend execution.
3. A replacement Worker reads the durable execution record.
4. It queries the backend for the execution state.
5. It attaches to an active execution, commits a terminal outcome, or applies
   the assignment's replay policy.

Execution records have their own lease fencing. A Session epoch grants no
permission to mutate an Execution record. Session commits and Execution
transitions remain separate protocol operations.

## Session Kernel and Runtime

The Session Kernel composes the durable semantic services:

- session stores and a Writer factory;
- the ledger and commit observers;
- the module registry;
- projections and projection caches;
- the Turn store adapter;
- Turn coordination;
- chatlog commands;
- artifacts, frozen content, and session history.

The Runtime assembles finite interpreters over the Kernel and Executor.
`SessionRuntime.Step` routes submitted inputs or advances the current Turn.
`Driver.Step` reconciles existing effects, delivers an available Outcome, or
calls `Loop.Advance`. `Compactions.Advance/Deliver` advances the compaction
protocol; pending maintenance gates ordinary request preparation.

`runtime.Inspect` reconstructs immediate work, future execution obligations,
and external response waits without ownership. It is a read model, not an
authority for commits. Domain transitions are decided against the Writer's
transactional projections and persisted through its fenced handle.
