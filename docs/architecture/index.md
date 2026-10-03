# Architecture

## Overview
Twilight is a carefully engineered, highly customizable agent built on a deployment-neutral architecture, allowing it to run in both local and cloud environments.

## Goal

Building an agent involves more than invoking models and tools. A practical agent needs a consistent execution model for sessions, runs, effects, storage, recovery, and interaction with external systems.

These concerns are often tightly coupled to a particular runtime or deployment environment, causing local and cloud agents to diverge into separate implementations.

Twilight addresses this by defining a shared, deployment-neutral Agent Core: a set of fundamental primitives and semantics from which both local and cloud agents can be built.


## Agent Core

Agent Core provides the fundamental execution and semantic primitives required to build an agent. It defines how agent state evolves, how external work is represented and executed, and how execution can be recovered across process and machine boundaries.

### Event-Sourced Semantic Model

Agent Core is built around an event-sourced semantic model.

Facts are the source of truth. Runtime state is not stored as an independent authority; it is deterministically derived by folding recorded facts.

Commands advance the system by producing new facts, while projections reconstruct the current state and other read models from the event history.

This gives the execution model a durable history and makes recovery, replay, inspection, and reconstruction explicit parts of the architecture.

### Core Abstractions

The core model is expressed through a small set of abstractions:

- **Session** — the long-lived semantic boundary of an agent interaction.
- **Run** — the state machine that describes the progression of an agent execution.
- **Effect** — an explicit description of work that must be performed outside the semantic state machine.
- **Execution** — the lifecycle and physical execution of an effect.
- **Outcome** — the observed result of an execution returned to the semantic layer.
- **Inbox** — the boundary through which external inputs and agent interactions enter the system.

Together, these abstractions separate the agent's semantic progression from the mechanisms used to perform external work.

### Semantic and Execution Boundaries

Agent Core separates semantic authority from execution authority.

The semantic side determines what the agent is doing and records that progression as facts. When external work is required, it produces an explicit effect.

The execution side is responsible for carrying out that effect through a model backend, tool backend, or another execution target, and reporting the resulting outcome.

```text
Semantic Authority
        │
        │ Effect
        ▼
Execution Authority
        │
        ▼
      Backend
        │
        │ Outcome
        ▼
Semantic Authority
```

This boundary prevents the agent's semantic state from depending on the lifetime of a particular process, worker, or backend.

### Deployment Model

The same semantic and execution model is used across deployment environments.

In a local deployment, these components can be colocated in a single process and backed by local durable storage and local executors.

In a cloud deployment, the same boundaries can be distributed across multiple processes and machines. Durable state, explicit ownership, and independent execution allow work to be reassigned, recovered, and horizontally scaled without changing the semantic model of the agent.

```text
                    Agent Core
                        │
                same semantic model
                 ┌──────┴──────┐
                 │             │
               Local          Cloud
                 │             │
          colocated parts   distributed parts
          local store       shared durable store
          local executor    executor cluster
                            horizontal scaling
```

Local deployment is therefore the simplest composition of the architecture, while cloud deployment distributes the same primitives across a larger execution environment.
