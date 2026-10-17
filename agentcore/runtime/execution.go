package runtime

import (
	"errors"

	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/driver"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/prompt"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/turn/effect"
	"github.com/felinics/twilight/agentcore/turn/frozen"
	"github.com/felinics/twilight/agentcore/turn/loop"
	"github.com/felinics/twilight/agentcore/turn/reconcile"
	"github.com/felinics/twilight/agentcore/turn/redispatch"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"
)

// ExecutionSources are durable semantic reads, independent of ownership.
type ExecutionSources struct {
	TurnStore   *sessionstore.SessionTurnStore
	Projections session.ProjectionReader
	Content     chatlog.ContentResolver
	Frozen      frozen.Store
}

// ExecutionConfig supplies interpretation and recovery policy. It deliberately
// contains no Session lease, heartbeat, polling interval, or Owner lifecycle.
type ExecutionConfig struct {
	Executor              effect.ExecutionPort
	Presets               preset.Catalog
	PromptBuilders        *prompt.Catalog
	MissingEffects        reconcile.MissingPolicy
	Redispatches          redispatch.Store
	MaxRedispatches       int
	Planner               driver.Planner
	ContextPolicy         driver.ContextPolicy
	MaxCompactionAttempts int
	TargetResolver        loop.TargetResolver
	Sink                  loop.EventSink
}

// Execution assembles finite protocol interpreters. Constructing it starts no
// goroutines and opening a Writer requires no execution-side Open/Close pair.
// Durable work discovery and ephemeral Activations are agent-layer concerns.
type Execution struct {
	Executor    effect.ExecutionPort
	Presets     preset.Catalog
	Compactions *driver.Compactions
	Driver      *driver.Driver
}

func NewExecution(cfg ExecutionConfig, src ExecutionSources) (*Execution, error) {
	if cfg.Executor == nil || cfg.PromptBuilders == nil {
		return nil, errors.New("runtime: executor and prompt builder catalog are required")
	}
	if src.TurnStore == nil || src.Projections == nil || src.Content == nil {
		return nil, errors.New("runtime: turn store, projections and content are required")
	}
	if cfg.MaxRedispatches < 0 || cfg.MaxCompactionAttempts < 0 {
		return nil, errors.New("runtime: negative recovery attempt limit")
	}
	if cfg.ContextPolicy != nil && src.Frozen == nil {
		return nil, errors.New("runtime: context policy requires a frozen store")
	}
	switch cfg.MissingEffects {
	case reconcile.DisposeMissing:
	case reconcile.RedispatchMissing:
		if cfg.Redispatches == nil {
			return nil, errors.New("runtime: redispatch policy requires a dispatch ledger")
		}
	default:
		return nil, errors.New("runtime: unknown missing effect policy")
	}
	presets := cfg.Presets
	if presets == nil {
		presets = preset.NewMemory()
	}
	maintenance := &driver.Compactions{Executor: cfg.Executor, Requests: src.TurnStore,
		Frozen: src.Frozen, Content: src.Content, Policy: cfg.ContextPolicy, MaxAttempts: cfg.MaxCompactionAttempts}
	loops := &driver.Loops{Executor: cfg.Executor, Presets: presets, PromptBuilders: cfg.PromptBuilders,
		Targets: cfg.TargetResolver, Sources: prompt.Sources{Projections: src.Projections, Content: src.Content},
		Planner: cfg.Planner, Candidates: maintenance}
	return &Execution{Executor: cfg.Executor, Presets: presets, Compactions: maintenance,
		Driver: &driver.Driver{TurnStore: src.TurnStore, Loops: loops, Compactions: maintenance, MissingEffects: cfg.MissingEffects,
			Redispatches: cfg.Redispatches, MaxRedispatches: cfg.MaxRedispatches, Sink: cfg.Sink}}, nil
}
