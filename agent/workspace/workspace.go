// Package workspace models a durable logical work world. It is an optional
// application capability: Agent Core does not import this package or interpret
// its revisions, snapshots, or runtime bindings.
package workspace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"

	"github.com/felinics/twilight/agent/environment"
	"github.com/felinics/twilight/agentcore/run"
)

// ID identifies one logical mutable work world. It remains stable when the
// physical runtime is replaced.
type ID string

// RevisionRef identifies the immutable base from which a workspace was
// materialized (for example, a git commit).
type RevisionRef string

// SnapshotRef identifies a durable workspace snapshot.
type SnapshotRef string

// BackendID identifies a runtime provider without exposing its API to the
// workspace domain.
type BackendID = environment.Backend

// EnvironmentRef identifies a provider environment. It is distinct from an
// execution/job ID: many executions may run in one environment.
type EnvironmentRef = environment.EnvironmentRef

// RuntimeBinding records the current physical materialization of a Workspace.
// Generation changes when the workspace is rebound to a new environment.
type RuntimeBinding = environment.Binding

// Snapshot is an immutable durable snapshot anchor produced by a provider.
// StateRef is provider-neutral at this boundary and is interpreted by the
// provider selected by Backend.
type Snapshot struct {
	Ref       SnapshotRef          `json:"ref"`
	Workspace ID                   `json:"workspace"`
	Backend   BackendID            `json:"backend"`
	StateRef  environment.StateRef `json:"stateRef"`
	Parent    *SnapshotRef         `json:"parent,omitempty"`
}

// Fork describes creation of a new logical workspace from an existing durable
// anchor. A fork receives a new identity and never aliases the source's mutable
// runtime binding.
type Fork struct {
	Source      ID          `json:"source"`
	Destination ID          `json:"destination"`
	Snapshot    SnapshotRef `json:"snapshot"`
	Base        RevisionRef `json:"base,omitempty"`
}

// Workspace is the durable logical work world operated on by an application.
// Filesystem contents and provider objects are not embedded here; only the
// anchors needed to materialize or restore them are retained.
type Workspace struct {
	ID       ID              `json:"id"`
	Project  string          `json:"project,omitempty"`
	Base     RevisionRef     `json:"base"`
	Snapshot *SnapshotRef    `json:"snapshot,omitempty"`
	Runtime  *RuntimeBinding `json:"runtime,omitempty"`
}

// Target returns the opaque Agent Core target for this workspace. The reverse
// dependency is intentional: this optional domain adapts to Core, never the
// other way around.
func (w Workspace) Target() run.TargetRef {
	return run.TargetRef{Kind: "workspace", ID: string(w.ID)}
}

// Errors of the Store contract.
var (
	// ErrNotFound: no Workspace or Snapshot has the identity.
	ErrNotFound = errors.New("workspace: not found")
	// ErrExists: Create or Fork named an ID that already has a Workspace.
	ErrExists = errors.New("workspace: already exists")
	// ErrGenerationConflict: UpdateRuntime found another RuntimeBinding
	// generation than the caller expected; another materialization won.
	ErrGenerationConflict = errors.New("workspace: runtime generation conflict")
	// ErrNothingToSnapshot: the Workspace has no environment to snapshot.
	ErrNothingToSnapshot = errors.New("workspace: no environment to snapshot")
)

// Snapshotter takes a Snapshot of a Workspace's current environment,
// records it and makes it the Workspace's latest (APP-WSP-7). The workspace
// backend implements it in its process; a remote one is reached through
// agent/workspace/http. A Workspace never materialized is
// ErrNothingToSnapshot; an unknown one ErrNotFound.
type Snapshotter interface {
	Snapshot(ctx context.Context, id ID) (Snapshot, error)
}

// Store persists logical workspaces and their durable anchors. Implementations
// may use a database, object store, or another application-owned repository.
// Every write is one transaction.
type Store interface {
	// Create stores a new Workspace; an ID already stored is ErrExists.
	Create(context.Context, Workspace) error
	// Get returns the Workspace; an unknown ID is ErrNotFound.
	Get(context.Context, ID) (Workspace, error)
	// Put replaces a stored Workspace; an unknown ID is ErrNotFound.
	Put(context.Context, Workspace) error
	// UpdateRuntime replaces the Workspace's RuntimeBinding when the stored
	// binding's Generation is expected (0 for no binding); otherwise nothing
	// is written and ErrGenerationConflict is returned. It is the write two
	// backend replicas race with when both materialize one workspace: the
	// loser attaches the winner's environment.
	UpdateRuntime(ctx context.Context, id ID, expected uint64, binding RuntimeBinding) error
	// PutSnapshot stores a Snapshot of a known Workspace; the same Ref
	// stored again with the same content is a no-op.
	PutSnapshot(context.Context, Snapshot) error
	// UpdateSnapshot makes a stored Snapshot of the Workspace its latest,
	// touching no other field: a partial write, so it never rolls back a
	// concurrent UpdateRuntime. An unknown Workspace, or a Snapshot the
	// store does not hold for it, is ErrNotFound.
	UpdateSnapshot(ctx context.Context, id ID, ref SnapshotRef) error
	// GetSnapshot returns the Snapshot; an unknown Ref is ErrNotFound.
	GetSnapshot(context.Context, SnapshotRef) (Snapshot, error)
	// Fork creates Destination from the Snapshot of Source: Project and
	// Snapshot carried over, Base from the Fork when given, no runtime
	// binding. An unknown Source or Snapshot is ErrNotFound, an existing
	// Destination ErrExists.
	Fork(context.Context, Fork) (Workspace, error)
}

// InheritedPolicy is what a Session does with a binding it inherited from
// its fork parent, decided when the Session is first opened (APP-WSP-5).
type InheritedPolicy uint8

const (
	// InheritShare keeps the parent's Workspace: the inherited binding
	// stands, nothing is written. The zero value.
	InheritShare InheritedPolicy = iota
	// InheritNone ends the inherited binding; the Session works in no
	// Workspace until bound.
	InheritNone
	// InheritAllocate binds a new, empty Workspace with the parent's
	// Project and Base.
	InheritAllocate
	// InheritClone binds a Workspace forked from the parent Workspace's
	// latest Snapshot, whenever that was taken.
	InheritClone
	// InheritRestore binds a Workspace forked from the Snapshot recorded on
	// the Session's history at the fork point (APP-WSP-7): the files as the
	// parent had them when the forked Turn began.
	InheritRestore
)

func (p InheritedPolicy) String() string {
	switch p {
	case InheritNone:
		return "none"
	case InheritAllocate:
		return "allocate"
	case InheritClone:
		return "clone"
	case InheritRestore:
		return "restore"
	default:
		return "share"
	}
}

// NewSnapshotRef mints a random SnapshotRef.
func NewSnapshotRef() SnapshotRef {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return SnapshotRef("snap-" + hex.EncodeToString(b[:]))
}

// NewID mints a random Workspace ID.
func NewID() ID {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return ID("ws-" + hex.EncodeToString(b[:]))
}
