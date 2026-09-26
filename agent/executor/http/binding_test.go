package http_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	executorhttp "github.com/felinics/twilight/agent/executor/http"
	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/executor/store/storetest"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/sdk"
)

// The binding is tested the way the Worker is: over a Worker whose backend
// is a Port-shaped fake. These doubles mirror the executor package's, kept
// here because agentcore's tests do not import the deployment's bindings.

// okBackend answers every Dispatch with a ModelSucceeded "ok" Outcome.
type okBackend struct {
	mu       sync.Mutex
	calls    int
	outcomes map[effect.AssignmentKey]chan effect.Outcome
}

func newOKBackend() *okBackend {
	return &okBackend{outcomes: make(map[effect.AssignmentKey]chan effect.Outcome)}
}

func (b *okBackend) Validate(context.Context, effect.Assignment) (*run.ToolFailure, error) {
	return nil, nil
}
func (b *okBackend) accept(a effect.Assignment) chan effect.Outcome {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	if b.outcomes[a.Key()] == nil {
		b.outcomes[a.Key()] = make(chan effect.Outcome, 1)
	}
	return b.outcomes[a.Key()]
}
func (b *okBackend) Dispatch(_ context.Context, a effect.Assignment) error {
	ch := b.accept(a)
	go func() {
		ch <- effect.Outcome{Key: a.Key(), Result: effect.ModelSucceeded{Result: sdk.ModelResult{Text: "ok"}}}
	}()
	return nil
}
func (b *okBackend) Attach(_ context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.outcomes[key] == nil {
		return effect.Attachment{State: effect.AttachmentMissing, Execution: effect.ExecutionNotFound}, nil
	}
	return effect.Attachment{State: effect.AttachmentActive, Execution: effect.ExecutionRunning, BackendAttached: true}, nil
}
func (b *okBackend) Abort(ctx context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	att, err := b.Attach(ctx, key)
	if err == nil && att.State == effect.AttachmentMissing {
		att = effect.Attachment{State: effect.AttachmentAborted, Execution: effect.ExecutionAborted}
	}
	return att, err
}
func (b *okBackend) GetStatus(_ context.Context, key effect.AssignmentKey) (effect.ExecutionStatus, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.outcomes[key] == nil {
		return effect.ExecutionNotFound, effect.ErrExecutionNotFound
	}
	return effect.ExecutionRunning, nil
}
func (b *okBackend) GetOutcome(ctx context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	b.mu.Lock()
	ch := b.outcomes[key]
	b.mu.Unlock()
	if ch == nil {
		return effect.Outcome{}, effect.ErrExecutionNotFound
	}
	select {
	case out := <-ch:
		return out, nil
	case <-ctx.Done():
		return effect.Outcome{}, ctx.Err()
	}
}
func (b *okBackend) Cancel(context.Context, effect.AssignmentKey) error { return nil }

// holdBackend accepts a Dispatch and never produces its Outcome until the
// test settles it.
type holdBackend struct{ *okBackend }

func (b *holdBackend) Dispatch(_ context.Context, a effect.Assignment) error { b.accept(a); return nil }
func (b *holdBackend) settle(a effect.Assignment, text string) {
	b.mu.Lock()
	b.outcomes[a.Key()] <- effect.Outcome{Key: a.Key(), Result: effect.ModelSucceeded{Result: sdk.ModelResult{Text: text}}}
	b.mu.Unlock()
}

// uncertainBackend loses every Dispatch response and answers the Outcome
// once ready is signalled.
type uncertainBackend struct {
	*okBackend
	ready chan struct{}
}

func (b *uncertainBackend) Dispatch(_ context.Context, a effect.Assignment) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	return effect.ErrDispatchUnknown
}
func (b *uncertainBackend) GetOutcome(ctx context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	select {
	case <-b.ready:
		return effect.Outcome{Key: key, Result: effect.ModelSucceeded{Result: sdk.ModelResult{Text: "accepted before response was lost"}}}, nil
	case <-ctx.Done():
		return effect.Outcome{}, ctx.Err()
	}
}

// failingCreateStore refuses every Append: the ledger store is unavailable.
type failingCreateStore struct{ store.Store }

func (failingCreateStore) Append(context.Context, store.Lease, effect.AssignmentKey, store.Commit) error {
	return errors.New("store unavailable")
}

// handlerTransport serves the client's requests from the Server's handler
// in-process.
type handlerTransport struct{ handler http.Handler }

func (t handlerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response := httptest.NewRecorder()
	t.handler.ServeHTTP(response, request)
	return response.Result(), nil
}

func routes(p effect.ExecutionPort) []executor.Route {
	return []executor.Route{executor.Default("test", executor.PortBackend(p))}
}

func inProcessClient(worker *executor.Worker) *executorhttp.Client {
	return &executorhttp.Client{BaseURL: "http://executor.invalid",
		HTTP: &http.Client{Transport: handlerTransport{handler: (&executorhttp.Server{Worker: worker}).Handler()}}}
}

func assignment() effect.Assignment {
	request := model.ModelRequest{Model: "m"}
	digest, err := schema.Canonical().DigestRequest(request)
	if err != nil {
		panic(err)
	}
	return effect.Assignment{Session: "s", RunID: "r", StepID: "step", Effect: "effect",
		Body: effect.ModelAssignment{Model: "m", Request: &request, RequestDigest: digest}}
}

func awaitOutcome(ctx context.Context, port effect.ExecutionPort, key effect.AssignmentKey) (effect.Outcome, error) {
	w := &effect.Watcher{Port: port, Poll: 5 * time.Millisecond}
	defer w.Close()
	return w.Await(ctx, key)
}

func modelText(out effect.Outcome) string {
	r, ok := out.ModelResult()
	if !ok {
		return ""
	}
	return r.Text
}

// A Dispatch whose backend response was lost is accepted over the wire: the
// server answers from the ledger, the replay starts nothing, and the
// Outcome arrives once the backend produces it.
func TestUncertainDispatchIsAcceptedOverHTTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	records := storetest.NewMap(nil)
	backend := &uncertainBackend{okBackend: newOKBackend(), ready: make(chan struct{})}
	defer close(backend.ready)
	worker, err := executor.NewWorker(ctx, records, routes(backend))
	if err != nil {
		t.Fatal(err)
	}
	client := inProcessClient(worker)
	a := assignment()
	if err := client.Dispatch(ctx, a); err != nil {
		t.Fatalf("dispatch error = %v", err)
	}
	r, _, _, err := records.Load(ctx, a.Key())
	if err != nil || r.State != effect.ExecutionDispatching || r.Outcome != nil {
		t.Fatalf("uncertain dispatch changed execution: %+v, %v", r, err)
	}
	if err := client.Dispatch(ctx, a); err != nil {
		t.Fatalf("acceptance replay = %v", err)
	}
	backend.mu.Lock()
	calls := backend.calls
	backend.mu.Unlock()
	if calls != 1 {
		t.Fatalf("backend calls = %d, want 1", calls)
	}
	select {
	case backend.ready <- struct{}{}:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	out, err := awaitOutcome(ctx, client, a.Key())
	if err != nil || modelText(out) != "accepted before response was lost" {
		t.Fatalf("eventual outcome = %+v, %v", out, err)
	}
}

func TestDispatchAssignmentConflictIsDefinite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	worker, err := executor.NewWorker(ctx, storetest.NewMap(nil), routes(newOKBackend()))
	if err != nil {
		t.Fatal(err)
	}
	client := inProcessClient(worker)
	a := assignment()
	if err := client.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, err := awaitOutcome(ctx, client, a.Key()); err != nil {
		t.Fatal(err)
	}
	a.Target = &run.TargetRef{Kind: "workspace", ID: "conflicting-target"}
	if err := client.Dispatch(ctx, a); err == nil || errors.Is(err, effect.ErrDispatchUnknown) {
		t.Fatalf("assignment conflict = %v, want definite rejection", err)
	}
}

func TestClientAndServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	worker, err := executor.NewWorker(ctx, storetest.NewMap(nil), routes(newOKBackend()))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((&executorhttp.Server{Worker: worker}).Handler())
	defer server.Close()
	client := &executorhttp.Client{BaseURL: server.URL}
	a := assignment()
	if err := client.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	status, err := client.GetStatus(ctx, a.Key())
	if err != nil || status == effect.ExecutionNotFound {
		t.Fatalf("status = %s, %v", status, err)
	}
	out, err := awaitOutcome(ctx, client, a.Key())
	if err != nil || modelText(out) != "ok" {
		t.Fatalf("HTTP outcome = %+v, %v", out, err)
	}
}

func TestControlEndpoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// The backend never answers, so Dispose races no watcher settlement.
	backend := &uncertainBackend{okBackend: newOKBackend(), ready: make(chan struct{})}
	defer close(backend.ready)
	worker, err := executor.NewWorker(ctx, storetest.NewMap(nil), routes(backend))
	if err != nil {
		t.Fatal(err)
	}
	client := inProcessClient(worker)
	a := assignment()
	if err := client.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := client.RecoverExecution(ctx, a.Key()); err != nil {
		t.Fatalf("recover = %v", err)
	}
	b := assignment()
	b.Effect = "effect-2"
	if err := client.Dispatch(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := client.Dispose(ctx, b.Key()); err != nil {
		t.Fatalf("dispose = %v", err)
	}
	out, err := awaitOutcome(ctx, client, b.Key())
	if _, isUnknown := out.Result.(effect.Unknown); err != nil || !isUnknown {
		t.Fatalf("disposed outcome = %+v, %v", out, err)
	}
	// The settled record is acknowledged and collected over the wire; the
	// collected Outcome reads as a definitive, classifiable error.
	if err := client.Acknowledge(ctx, a.Key()); err == nil || !strings.Contains(err.Error(), "409") {
		t.Fatalf("acknowledge of an executing record = %v, want 409", err)
	}
	if err := client.Acknowledge(ctx, b.Key()); err != nil {
		t.Fatalf("acknowledge = %v", err)
	}
	if _, err := awaitOutcome(ctx, client, b.Key()); !errors.Is(err, effect.ErrOutcomeUnavailable) {
		t.Fatalf("collected outcome over http = %v, want ErrOutcomeUnavailable", err)
	}
	c := assignment()
	c.Effect = "effect-3"
	if _, err := awaitOutcome(ctx, client, c.Key()); !errors.Is(err, effect.ErrExecutionNotFound) {
		t.Fatalf("unknown key over http = %v, want ErrExecutionNotFound", err)
	}
	// The tombstone travels over the wire: an aborted key answers aborted,
	// rejects a later Dispatch with a definite 409, and its Outcome reads
	// as a definitive error (RUN-EXE-16).
	if closed, err := client.Abort(ctx, c.Key()); err != nil || closed.State != effect.AttachmentAborted {
		t.Fatalf("abort over http = %+v %v", closed, err)
	}
	if err := client.Dispatch(ctx, c); err == nil || !strings.Contains(err.Error(), "409") || errors.Is(err, effect.ErrDispatchUnknown) || errors.Is(err, effect.ErrDispatchRetryable) {
		t.Fatalf("dispatch of an aborted key over http = %v, want a definite 409", err)
	}
	if _, err := awaitOutcome(ctx, client, c.Key()); !errors.Is(err, effect.ErrOutcomeUnavailable) {
		t.Fatalf("outcome of an aborted key over http = %v, want ErrOutcomeUnavailable", err)
	}
}

// The Worker's refusal classes cross the wire as statuses and come back as
// the same sentinels: a retryable refusal is a 503 and ErrDispatchRetryable,
// a definite rejection is a 400 and a plain error; neither is unknown
// (RUN-EXE-3, CLD-WIR-2).
func TestDispatchRefusalStatus(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name       string
		records    store.Store
		assignment effect.Assignment
		retryable  bool
	}{
		{"record store unavailable", failingCreateStore{storetest.NewMap(nil)}, assignment(), true},
		{"assignment without body", storetest.NewMap(nil), effect.Assignment{Session: "s", RunID: "r", Effect: "e"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			worker, err := executor.NewWorker(ctx, tc.records, routes(newOKBackend()))
			if err != nil {
				t.Fatal(err)
			}
			defer worker.Close()
			err = inProcessClient(worker).Dispatch(ctx, tc.assignment)
			if err == nil || errors.Is(err, effect.ErrDispatchUnknown) || errors.Is(err, effect.ErrDispatchRetryable) != tc.retryable {
				t.Fatalf("dispatch = %v, want retryable=%v and not unknown", err, tc.retryable)
			}
		})
	}
}

// The settlement stream over a real server: a subscriber under the hub's
// epoch sees a's settlement, a resubscription after its sequence delivers
// what was missed, and the Worker's Close ends the stream (RUN-EXE-17).
func TestSettlementStreamOverHTTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	backend := &holdBackend{newOKBackend()}
	hub := executor.NewSettlementHub("worker-a/1", 0)
	worker, err := executor.NewWorker(ctx, storetest.NewMap(nil), routes(backend), executor.WorkerOptions{ID: "worker-a", Settlements: hub})
	if err != nil {
		t.Fatal(err)
	}
	// A real server: the recorder transport returns a response only when the
	// handler finishes, which a stream does not.
	server := httptest.NewServer((&executorhttp.Server{Worker: worker}).Handler())
	defer server.Close()
	client := &executorhttp.Client{BaseURL: server.URL}
	a, b := assignment(), assignment()
	b.Effect = "second"
	for _, asg := range []effect.Assignment{a, b} {
		if err := worker.Dispatch(ctx, asg); err != nil {
			t.Fatal(err)
		}
	}
	began := time.Now()
	if _, err := client.GetOutcome(ctx, a.Key()); !errors.Is(err, effect.ErrOutcomeNotReady) {
		t.Fatalf("read of an unsettled execution = %v, want ErrOutcomeNotReady", err)
	}
	if waited := time.Since(began); waited > time.Second {
		t.Fatalf("read waited %v; a read does not wait", waited)
	}
	seen := make(chan effect.Settlement, 4)
	streamDone := make(chan error, 1)
	go func() {
		streamDone <- client.Settlements(ctx, hub.Epoch(), 0, func(s effect.Settlement) bool {
			if s.Key == (effect.AssignmentKey{}) {
				return true // the stream's announcement of its epoch and head
			}
			seen <- s
			return s.Key != a.Key()
		})
	}()
	backend.settle(a, "first")
	var first effect.Settlement
	select {
	case first = <-seen:
	case <-ctx.Done():
		t.Fatal("no settlement notice for a")
	}
	if first.Key != a.Key() || first.Sequence == 0 || first.Epoch != hub.Epoch() {
		t.Fatalf("settlement = %+v", first)
	}
	if err := <-streamDone; err != nil {
		t.Fatalf("stream ended with %v after fn stopped it", err)
	}
	if out, err := client.GetOutcome(ctx, a.Key()); err != nil || modelText(out) != "first" {
		t.Fatalf("read after the notice = %+v, %v", out, err)
	}
	backend.settle(b, "second")
	if _, err := awaitOutcome(ctx, worker, b.Key()); err != nil {
		t.Fatalf("b did not settle: %v", err)
	}
	var missed []effect.Settlement
	streamDone = make(chan error, 1)
	go func() {
		streamDone <- client.Settlements(ctx, first.Epoch, first.Sequence, func(s effect.Settlement) bool {
			if s.Key != (effect.AssignmentKey{}) {
				missed = append(missed, s)
			}
			return true
		})
	}()
	time.Sleep(50 * time.Millisecond)
	worker.Close()
	if err := <-streamDone; err != nil {
		t.Fatalf("stream after Close = %v, want a clean end", err)
	}
	if len(missed) != 1 || missed[0].Key != b.Key() || missed[0].Sequence != first.Sequence+1 {
		t.Fatalf("resubscription after %d = %+v, want b's notice alone", first.Sequence, missed)
	}
}
