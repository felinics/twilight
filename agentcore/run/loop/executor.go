package loop

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/felinics/twilight/agentcore/executor/notice"
	run "github.com/felinics/twilight/agentcore/run"
	effect "github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/frozen"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/run/model/sdkconv"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/sdk"
)

// The Loop package keeps aliases for the protocol types so existing Run/Loop
// call sites remain source-compatible. The definitions live in run/effect;
// this package owns only the local execution implementation and Loop helpers.
type AssignmentKind = effect.AssignmentKind

type AssignmentKey = effect.AssignmentKey

type ModelAssignment = effect.ModelAssignment

type ToolAssignment = effect.ToolAssignment

type Assignment = effect.Assignment

type Outcome = effect.Outcome
type OutcomeResult = effect.OutcomeResult
type ModelSucceeded = effect.ModelSucceeded
type ModelFailed = effect.ModelFailed
type Cancelled = effect.Cancelled
type Unknown = effect.Unknown
type FailureCode = effect.FailureCode

type ExecutionStatus = effect.ExecutionStatus
type AttachmentState = effect.AttachmentState
type Attachment = effect.Attachment

type Executor = effect.ExecutionPort

// Acknowledger is effect.Acknowledger: the optional retention hint an
// Executor may implement (RUN-EXE-13).
type Acknowledger = effect.Acknowledger

const (
	AssignmentModel          = effect.AssignmentModel
	AssignmentTool           = effect.AssignmentTool
	ExecutionNotFound        = effect.ExecutionNotFound
	ExecutionAccepted        = effect.ExecutionAccepted
	ExecutionRunning         = effect.ExecutionRunning
	ExecutionCancelRequested = effect.ExecutionCancelRequested
	ExecutionCompleted       = effect.ExecutionCompleted
	ExecutionFailed          = effect.ExecutionFailed
	ExecutionCancelled       = effect.ExecutionCancelled
	ExecutionUnknown         = effect.ExecutionUnknown
	ExecutionAborted         = effect.ExecutionAborted

	FailureExecutor           = effect.FailureExecutor
	FailureFrozenValueMissing = effect.FailureFrozenValueMissing
	FailureMalformedRequest   = effect.FailureMalformedRequest
	FailureDeadline           = effect.FailureDeadline

	AttachmentMissing  = effect.AttachmentMissing
	AttachmentActive   = effect.AttachmentActive
	AttachmentOrphaned = effect.AttachmentOrphaned
	AttachmentTerminal = effect.AttachmentTerminal
	AttachmentAborted  = effect.AttachmentAborted
)

var (
	ErrExecutionNotFound = effect.ErrExecutionNotFound
	ErrOutcomeNotReady   = effect.ErrOutcomeNotReady
	ErrDispatchUnknown   = effect.ErrDispatchUnknown
)

// ErrExecutorRejected reports an assignment the executor would not start.
var ErrExecutorRejected = errors.New("agent: loop: executor rejected the assignment")

// ErrModelUnavailable reports a model assignment the executor cannot serve,
// found by Validate before the start barrier: the step stays Prepared.
var ErrModelUnavailable = errors.New("agent: loop: executor cannot serve the model")

// --- LocalExecutor ------------------------------------------------------------

// LocalExecutor runs effects in goroutines of one process. It is the compact
// in-process implementation of the message-shaped Executor port. Its records
// are process-scoped: after a process restart Attach cannot find an old
// assignment. Durable cross-worker recovery belongs to executor.Worker.
// LocalExecutor is the colocated execution backend: model and tool effects
// run in goroutines of this process against the catalogs. It implements the
// executor Backend contract structurally -- Prepare derives the Ref from the
// AssignmentKey, the other methods address that Ref -- and keeps a bounded
// table of terminal entries for the Worker's outcome reads. It does not
// implement effect.ExecutionPort: Agent Core reaches it through executor.Worker.
type LocalExecutor struct {
	models    ModelCatalog
	tools     ToolCatalog
	sink      effect.ProgressSink
	streaming bool

	// notices announces each ref that settled (notice.Source); the Worker
	// waits on it instead of blocking in Outcome.
	notices *notice.RefHub

	mu       sync.Mutex
	inflight map[string]*inflight
	// terminal lists the closed entries oldest first; once more than retain
	// of them are held the oldest are dropped from inflight.
	terminal []string
	retain   int
}

// DefaultRetainedOutcomes bounds the terminal records a LocalExecutor keeps
// for idempotent reads (RUN-EXE-3); records still executing are never dropped.
const DefaultRetainedOutcomes = 1024

type inflight struct {
	runID   run.RunID
	digest  run.Digest
	cancel  context.CancelFunc
	done    chan struct{}
	outcome Outcome
	closed  bool
}

// NewLocalExecutor builds the in-process executor. sink receives progress
// frames (model deltas, tool progress; RUN-EXE-12), normally the Worker's
// ProgressHub; nil discards them. streaming selects StreamingModelInvoker
// when the invoker offers it.
func NewLocalExecutor(models ModelCatalog, tools ToolCatalog, sink effect.ProgressSink, streaming bool) (*LocalExecutor, error) {
	if models == nil {
		return nil, errors.New("agent: loop: nil model catalog")
	}
	if tools == nil {
		return nil, errors.New("agent: loop: nil tool catalog")
	}
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return nil, fmt.Errorf("agent: loop: executor epoch: %w", err)
	}
	return &LocalExecutor{models: models, tools: tools, sink: sink, streaming: streaming,
		notices:  notice.NewRefHub("local/"+hex.EncodeToString(raw[:]), 0),
		inflight: make(map[string]*inflight), retain: DefaultRetainedOutcomes}, nil
}

// RefOf is the Ref LocalExecutor derives for an AssignmentKey: the key
// itself, encoded. Prepare is therefore idempotent and stateless.
func RefOf(key AssignmentKey) string {
	raw, err := json.Marshal(key)
	if err != nil {
		panic("agent: loop: assignment key does not marshal: " + err.Error())
	}
	return string(raw)
}

// Prepare derives the Ref of the Assignment without starting it.
func (e *LocalExecutor) Prepare(_ context.Context, a Assignment) (string, error) {
	return RefOf(a.Key()), nil
}

// Restart derives the Ref of the next generation of the attempt: the key's
// Ref with a generation suffix, so a re-dispatched step gets its own entry
// and the previous one stays readable until retention drops it. Whether a
// tool may be re-dispatched is the Worker's decision from the Assignment's
// Replay policy (RUN-EXE-9); Restart only derives the Ref.
func (e *LocalExecutor) Restart(_ context.Context, previous string, a Assignment) (string, error) {
	base := RefOf(a.Key())
	gen := 1
	if strings.HasPrefix(previous, base+"#") {
		if n, err := strconv.Atoi(strings.TrimPrefix(previous, base+"#")); err == nil {
			gen = n + 1
		}
	}
	return base + "#" + strconv.Itoa(gen), nil
}

// SetRetainedOutcomes bounds the terminal entries kept for Attach, Status
// and Outcome after an execution closes; n < 1 keeps one. A dropped entry
// answers missing.
func (e *LocalExecutor) SetRetainedOutcomes(n int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.retain = max(n, 1)
	e.evictLocked()
}

// evictLocked drops the oldest terminal records beyond the retention bound;
// e.mu must be held.
func (e *LocalExecutor) evictLocked() {
	for len(e.terminal) > e.retain {
		delete(e.inflight, e.terminal[0])
		e.terminal = e.terminal[1:]
	}
}

// Validate is the pre-start check (RUN-EXE-5): tools per RUN-LOP-4, models
// by resolving the ModelRef in the catalog so a missing model fails before
// any start fact is written (RUN-LOP-3).
func (e *LocalExecutor) Validate(_ context.Context, a Assignment) (*run.ToolFailure, error) {
	switch body := a.Body.(type) {
	case ModelAssignment:
		invoker, err := e.models.ResolveModel(body.Model)
		if err != nil {
			return &run.ToolFailure{Class: run.FailureProvider, Message: err.Error()}, nil
		}
		if invoker == nil {
			return &run.ToolFailure{Class: run.FailureProvider, Message: "model catalog returned a nil invoker"}, nil
		}
		return nil, nil
	case ToolAssignment:
		_, failure := e.resolveTool(&body)
		return failure, nil
	default:
		return &run.ToolFailure{Class: run.FailureProvider, Message: "assignment without body"}, nil
	}
}

func (e *LocalExecutor) resolveTool(t *ToolAssignment) (ExecutableTool, *run.ToolFailure) {
	tool, resolveErr := e.tools.ResolveTool(t.ToolRef)
	if resolveErr != nil {
		return nil, &run.ToolFailure{Class: run.FailureToolLookup, Message: resolveErr.Error()}
	}
	if tool == nil {
		return nil, &run.ToolFailure{Class: run.FailureToolLookup, Message: "tool catalog returned a nil tool"}
	}
	toolDef, freezeErr := sdkconv.FreezeToolDefinition(tool.Definition())
	if freezeErr != nil {
		return nil, &run.ToolFailure{Class: run.FailureDefinitionMismatch, Message: freezeErr.Error()}
	}
	defDigest, digestErr := schema.Canonical().DigestToolDefinition(toolDef)
	if digestErr != nil {
		return nil, &run.ToolFailure{Class: run.FailureDefinitionMismatch, Message: digestErr.Error()}
	}
	switch {
	case tool.Ref() != t.ToolRef || defDigest != t.DefinitionDigest:
		return nil, &run.ToolFailure{Class: run.FailureDefinitionMismatch, Message: "tool definition digest mismatch"}
	case tool.ResponsePolicy() != t.Policy:
		return nil, &run.ToolFailure{Class: run.FailureDefinitionMismatch, Message: "response policy mismatch"}
	case tool.Replay() != t.Replay:
		return nil, &run.ToolFailure{Class: run.FailureDefinitionMismatch, Message: "replay policy mismatch"}
	case tool.Placement() != t.Placement:
		return nil, &run.ToolFailure{Class: run.FailureDefinitionMismatch, Message: "placement mismatch"}
	}
	if argErr := tool.ValidateArguments(t.Arguments); argErr != nil {
		return nil, &run.ToolFailure{Class: run.FailureInvalidArguments, Message: argErr.Error()}
	}
	return tool, nil
}

// Start begins the effect in a goroutine under ref. The effect's context is
// derived from ctx's values but not its cancellation: the caller's request
// ends when Start returns, while the effect ends by Outcome or Cancel. The
// outcome is retained in the entry and read through Outcome; it is not
// delivered through a process-local callback. Start of a ref already held is
// a no-op when the Assignment is the same, a rejection otherwise.
func (e *LocalExecutor) Start(ctx context.Context, ref string, a Assignment) error {
	key := ref
	digest, err := a.Digest()
	if err != nil {
		return fmt.Errorf("%w: assignment digest: %w", ErrExecutorRejected, err)
	}
	// Check before resolving catalogs or fetching frozen content: an
	// idempotent retry must not depend on transient execution dependencies.
	e.mu.Lock()
	if existing, ok := e.inflight[key]; ok {
		e.mu.Unlock()
		if existing.digest == digest {
			return nil
		}
		return fmt.Errorf("%w: assignment key reused with different content", ErrExecutorRejected)
	}
	e.mu.Unlock()

	var execute func(context.Context) Outcome
	switch body := a.Body.(type) {
	case ModelAssignment:
		invoker, err := e.models.ResolveModel(body.Model)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrExecutorRejected, err)
		}
		if invoker == nil {
			return fmt.Errorf("%w: model catalog returned a nil invoker", ErrExecutorRejected)
		}
		// Dispatch MUST carry the inline request payload (RUN-EXE-7): the
		// digest-only reconstruction of AssignmentFromTarget never enters
		// Dispatch, so a missing body is a definite rejection here.
		if body.Request == nil {
			return fmt.Errorf("%w: model assignment without an inline request payload", ErrExecutorRejected)
		}
		frozenRequest := *body.Request
		got, err := schema.Canonical().DigestRequest(frozenRequest)
		if err != nil {
			return fmt.Errorf("%w: request digest: %w", ErrExecutorRejected, err)
		}
		if got != body.RequestDigest || frozenRequest.Model != string(body.Model) {
			return fmt.Errorf("%w: model request digest or model mismatch", ErrExecutorRejected)
		}
		execute = func(ctx context.Context) Outcome { return e.runModel(ctx, a, &frozenRequest, invoker) }
	case ToolAssignment:
		tool, failure := e.resolveTool(&body)
		if failure != nil {
			return fmt.Errorf("%w: %s: %s", ErrExecutorRejected, failure.Class, failure.Message)
		}
		binding := body
		execute = func(ctx context.Context) Outcome { return e.runTool(ctx, a, binding, tool) }
	default:
		return fmt.Errorf("%w: assignment without body", ErrExecutorRejected)
	}

	effectCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	entry := &inflight{runID: a.RunID, digest: digest, cancel: cancel, done: make(chan struct{})}
	e.mu.Lock()
	if existing, dup := e.inflight[key]; dup {
		e.mu.Unlock()
		cancel()
		if existing.digest == digest {
			return nil // idempotent retry of the same accepted assignment
		}
		return fmt.Errorf("%w: assignment key reused with different content", ErrExecutorRejected)
	}
	e.inflight[key] = entry
	e.mu.Unlock()

	go func() {
		out := execute(effectCtx)
		out.Key = a.Key()
		if effectCtx.Err() != nil {
			out.Result = cancelled(out.Result, effectCtx.Err())
		}
		e.mu.Lock()
		entry.outcome = out
		entry.closed = true
		close(entry.done)
		e.terminal = append(e.terminal, key)
		e.evictLocked()
		e.mu.Unlock()
		e.notices.Record(key)
		cancel()
	}()
	return nil
}

// Attach answers for refs this process still runs or has completed. It only
// observes an existing entry and never starts a second effect; an unknown
// Ref is missing, a proof because executions die with the process while
// their records do not.
func (e *LocalExecutor) Attach(_ context.Context, ref string) (effect.Attachment, error) {
	e.mu.Lock()
	entry, ok := e.inflight[ref]
	if !ok {
		e.mu.Unlock()
		return effect.Attachment{State: effect.AttachmentMissing, Execution: effect.ExecutionNotFound}, nil
	}
	closed := entry.closed
	out := entry.outcome
	e.mu.Unlock()
	if !closed {
		return effect.Attachment{State: effect.AttachmentActive, Execution: effect.ExecutionRunning, BackendAttached: true}, nil
	}
	return effect.Attachment{State: effect.AttachmentTerminal, Execution: out.Status(), BackendAttached: true}, nil
}

// Status returns the current process-scoped execution status of ref.
func (e *LocalExecutor) Status(_ context.Context, ref string) (ExecutionStatus, error) {
	e.mu.Lock()
	entry, ok := e.inflight[ref]
	if !ok {
		e.mu.Unlock()
		return ExecutionNotFound, ErrExecutionNotFound
	}
	closed := entry.closed
	out := entry.outcome
	e.mu.Unlock()
	if !closed {
		return ExecutionRunning, nil
	}
	return out.Status(), nil
}

// cancelled is what a result becomes when the effect's context ended before
// it settled: a success is kept -- the effect happened -- anything else is
// the cancellation.
func cancelled(result OutcomeResult, cause error) OutcomeResult {
	switch result.(type) {
	case effect.ModelSucceeded, effect.ToolExecutionSucceeded:
		return result
	default:
		return effect.Cancelled{Message: cause.Error()}
	}
}

// Outcome waits for and returns the stable outcome of ref. A terminal entry
// stays readable until the retention bound drops it (SetRetainedOutcomes); a
// dropped entry is ErrExecutionNotFound.
func (e *LocalExecutor) Outcome(_ context.Context, ref string) (Outcome, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	entry, ok := e.inflight[ref]
	if !ok {
		return Outcome{}, ErrExecutionNotFound
	}
	if !entry.closed {
		return Outcome{}, ErrOutcomeNotReady
	}
	return entry.outcome, nil
}

// Settled streams the refs whose Outcome became readable (notice.Source).
func (e *LocalExecutor) Settled(ctx context.Context, epoch string, after uint64, fn func(notice.Ref) bool) error {
	return e.notices.Settled(ctx, epoch, after, fn)
}

// Cancel requests cancellation of ref; its Outcome remains observable.
func (e *LocalExecutor) Cancel(_ context.Context, ref string) error {
	e.mu.Lock()
	entry, ok := e.inflight[ref]
	e.mu.Unlock()
	if ok {
		entry.cancel()
	}
	return nil
}

// InFlight reports the number of assignments still executing (tests).
func (e *LocalExecutor) InFlight() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for _, entry := range e.inflight {
		if !entry.closed {
			n++
		}
	}
	return n
}

func (e *LocalExecutor) runModel(ctx context.Context, a Assignment, frozenRequest *model.ModelRequest, invoker ModelInvoker) Outcome {
	sdkRequest, err := sdkconv.ModelRequest(*frozenRequest)
	if err != nil {
		return Outcome{Result: effect.ModelFailed{Code: effect.FailureMalformedRequest, Message: "frozen request cannot be materialized: " + err.Error()}}
	}
	result, err := e.invokeModel(ctx, invoker, &sdkRequest, a)
	if err != nil {
		return Outcome{Result: modelFailure(err)}
	}
	return Outcome{Result: effect.ModelSucceeded{Result: result}}
}

// modelFailure classifies a model invocation error into the wire-stable
// failure codes (RUN-EXE-11): the executor's own conditions first, then the
// provider's HTTP status or the transport through effect.ClassifyModelError.
func modelFailure(err error) OutcomeResult {
	switch {
	case errors.Is(err, frozen.ErrMissing):
		return effect.ModelFailed{Code: effect.FailureFrozenValueMissing, Message: err.Error()}
	case errors.Is(err, context.Canceled):
		return effect.Cancelled{Message: err.Error()}
	case errors.Is(err, context.DeadlineExceeded):
		return effect.ModelFailed{Code: effect.FailureDeadline, Message: err.Error()}
	default:
		return effect.ModelFailed{Code: effect.ClassifyModelError(err), Message: err.Error()}
	}
}

func (e *LocalExecutor) invokeModel(ctx context.Context, invoker ModelInvoker, req *sdk.Request, a Assignment) (sdk.ModelResult, error) {
	if e.streaming {
		if streamer, ok := invoker.(StreamingModelInvoker); ok {
			stream, err := streamer.Stream(ctx, *req)
			if err != nil {
				return sdk.ModelResult{}, err
			}
			// The range has an explicit ctx escape: a stream that stops
			// sending without closing Parts must not block cancellation. The
			// assembler behind Parts tolerates abandonment: it stops forwarding
			// once ctx is done and drains the provider.
			emitDelta := func(kind effect.ProgressKind, payload any) {
				if e.sink == nil {
					return
				}
				e.sink.Publish(ctx, effect.ProgressFrame{Key: a.Key(), Kind: kind, Payload: mustJSON(payload)})
			}
		consume:
			for {
				select {
				case part, open := <-stream.Parts:
					if !open {
						break consume
					}
					switch p := part.(type) {
					case *sdk.TextDeltaPart:
						emitDelta(effect.ProgressTextDelta, p.Text)
					case *sdk.ReasoningDeltaPart:
						emitDelta(effect.ProgressReasoningDelta, p.Text)
					}
				case <-ctx.Done():
					return sdk.ModelResult{}, ctx.Err()
				}
			}
			result, err := stream.Result()
			if err != nil {
				return sdk.ModelResult{}, err
			}
			if result == nil {
				return sdk.ModelResult{}, errors.New("agent: loop: stream returned no result")
			}
			return *result, nil
		}
	}
	return invoker.Generate(ctx, *req)
}

func (e *LocalExecutor) runTool(ctx context.Context, a Assignment, t ToolAssignment, tool ExecutableTool) Outcome {
	req := ToolExecutionRequest{
		RunID:            a.RunID,
		StepID:           a.StepID,
		CallID:           a.CallID,
		Effect:           a.Effect,
		ToolRef:          t.ToolRef,
		DefinitionDigest: t.DefinitionDigest,
		Arguments:        t.Arguments,
		Target:           cloneTarget(a.Target),
		Progress:         &progressSink{sink: e.sink, key: a.Key()},
	}
	return Outcome{Result: executeToolSafely(ctx, tool, &req)}
}

func cloneTarget(target *run.TargetRef) *run.TargetRef {
	if target == nil {
		return nil
	}
	cloned := *target
	return &cloned
}

// executeToolSafely runs an application tool and converts a panic into
// ToolExecutionUnknown: the effect may have happened before the panic, and a
// crashing tool must not take down every run in the process.
func executeToolSafely(ctx context.Context, tool ExecutableTool, req *ToolExecutionRequest) (outcome ToolExecutionOutcome) {
	defer func() {
		if r := recover(); r != nil {
			outcome = ToolExecutionUnknown{Failure: run.ToolFailure{
				Class:   run.FailureEffectUnknown,
				Message: fmt.Sprintf("tool panic: %v", r),
			}}
		}
	}()
	return tool.Execute(ctx, *req)
}
