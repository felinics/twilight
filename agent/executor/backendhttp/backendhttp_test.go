package backendhttp_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/executor/backendhttp"
	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/executor/notice"
	"github.com/felinics/twilight/agentcore/executor/store/storetest"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/sdk"
)

// fakeBackend is a stateless backend as the wire expects one: an in-flight
// table keyed by ref, an Outcome per ref the test releases, one progress
// frame per start, and a fresh ref on every Restart.
type fakeBackend struct {
	mu       sync.Mutex
	hub      *executor.ProgressHub
	notices  *notice.RefHub
	inflight map[string]*fakeRun
	starts   int
	restarts int
	text     string
}

type fakeRun struct {
	done    chan struct{}
	release chan struct{}
	out     effect.Outcome
}

func newFakeBackend(text string) *fakeBackend {
	return &fakeBackend{hub: executor.NewProgressHub(0), notices: notice.NewRefHub("fake/"+text, 0), inflight: map[string]*fakeRun{}, text: text}
}

func (b *fakeBackend) Validate(context.Context, effect.Assignment) (*run.ToolFailure, error) {
	return nil, nil
}
func (b *fakeBackend) Prepare(_ context.Context, a effect.Assignment) (string, error) {
	return "ref/" + string(a.Effect), nil
}
func (b *fakeBackend) Start(_ context.Context, ref string, a effect.Assignment) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.inflight[ref]; ok {
		return nil
	}
	b.starts++
	r := &fakeRun{done: make(chan struct{}), release: make(chan struct{})}
	b.inflight[ref] = r
	go func() {
		b.hub.Publish(context.Background(), effect.ProgressFrame{Key: a.Key(), Kind: effect.ProgressTextDelta, Payload: []byte(`"working"`)})
		<-r.release
		b.mu.Lock()
		r.out = effect.Outcome{Key: a.Key(), Result: effect.ModelSucceeded{Result: sdk.ModelResult{Text: b.text}}}
		close(r.done)
		b.mu.Unlock()
		b.hub.End(a.Key())
		b.notices.Record(ref)
	}()
	return nil
}
func (b *fakeBackend) Restart(_ context.Context, _ string, _ effect.Assignment) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.restarts++
	return fmt.Sprintf("ref/restart-%d", b.restarts), nil
}
func (b *fakeBackend) Attach(_ context.Context, ref string) (effect.Attachment, error) {
	b.mu.Lock()
	r, ok := b.inflight[ref]
	b.mu.Unlock()
	if !ok {
		return effect.Attachment{State: effect.AttachmentMissing, Execution: effect.ExecutionNotFound}, nil
	}
	select {
	case <-r.done:
		return effect.Attachment{State: effect.AttachmentTerminal, Execution: effect.ExecutionCompleted, BackendAttached: true}, nil
	default:
		return effect.Attachment{State: effect.AttachmentActive, Execution: effect.ExecutionRunning, BackendAttached: true}, nil
	}
}
func (b *fakeBackend) Status(ctx context.Context, ref string) (effect.ExecutionStatus, error) {
	att, _ := b.Attach(ctx, ref)
	if att.State == effect.AttachmentMissing {
		return effect.ExecutionNotFound, effect.ErrExecutionNotFound
	}
	return att.Execution, nil
}
func (b *fakeBackend) Outcome(_ context.Context, ref string) (effect.Outcome, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, ok := b.inflight[ref]
	if !ok {
		return effect.Outcome{}, effect.ErrExecutionNotFound
	}
	select {
	case <-r.done:
		return r.out, nil
	default:
		return effect.Outcome{}, effect.ErrOutcomeNotReady
	}
}

func (b *fakeBackend) Settled(ctx context.Context, epoch string, after uint64, fn func(notice.Ref) bool) error {
	return b.notices.Settled(ctx, epoch, after, fn)
}

func (b *fakeBackend) Cancel(context.Context, string) error { return nil }

// releaseAll lets every in-flight run settle.
func (b *fakeBackend) releaseAll() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, r := range b.inflight {
		select {
		case <-r.release:
		default:
			close(r.release)
		}
	}
}

func (b *fakeBackend) counts() (starts, restarts int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.starts, b.restarts
}

func modelAssignment(effectID run.EffectID) effect.Assignment {
	request := &model.ModelRequest{Model: "m", Messages: []model.Message{{Role: "user", Content: []model.MessagePart{{Type: "text", Text: "hi"}}}}}
	digest, err := schema.Canonical().DigestRequest(*request)
	if err != nil {
		panic(err)
	}
	return effect.Assignment{Session: "s", RunID: "r", StepID: "step", Effect: effectID,
		Body: effect.ModelAssignment{Model: "m", Request: request, RequestDigest: digest}}
}

// await waits for key's Outcome with a Watcher over port: the port's
// settlement stream when it has one, a read every poll otherwise.
func await(ctx context.Context, port effect.ExecutionPort, key effect.AssignmentKey, poll time.Duration) (effect.Outcome, error) {
	w := &effect.Watcher{Port: port, Poll: poll}
	defer w.Close()
	return w.Await(ctx, key)
}

func modelText(out effect.Outcome) string {
	if r, ok := out.ModelResult(); ok {
		return r.Text
	}
	return ""
}

// A Worker routing to a Client sees the remote backend exactly as an
// in-process one: Dispatch starts it, progress frames arrive through the
// Worker's Progress, the Outcome is read once the backend settles, and the
// Client's wait between reads is a notice, not a held request.
func TestWorkerOverBackendWire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	backend := newFakeBackend("remote")
	server := httptest.NewServer((&backendhttp.Server{Backend: backend, Progress: backend.hub}).Handler())
	defer server.Close()
	client := &backendhttp.Client{BaseURL: server.URL}
	worker, err := executor.NewWorker(ctx, storetest.NewMap(nil), []executor.Route{executor.Default("remote", client)}, executor.WorkerOptions{ID: "w"})
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	a := modelAssignment("e1")
	if err := worker.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	var frames []effect.ProgressFrame
	progressCtx, stopProgress := context.WithTimeout(ctx, 2*time.Second)
	defer stopProgress()
	_ = worker.Progress(progressCtx, a.Key(), 0, func(f effect.ProgressFrame) bool {
		frames = append(frames, f)
		return f.Kind != effect.ProgressTextDelta
	})
	if len(frames) == 0 || frames[len(frames)-1].Kind != effect.ProgressTextDelta {
		t.Fatalf("relayed progress = %+v, want the backend's text delta", frames)
	}
	if _, err := worker.GetOutcome(ctx, a.Key()); !errors.Is(err, effect.ErrOutcomeNotReady) {
		t.Fatalf("read before settlement = %v, want not ready", err)
	}
	began := time.Now()
	backend.releaseAll()
	out, err := await(ctx, worker, a.Key(), time.Minute)
	if err != nil || modelText(out) != "remote" {
		t.Fatalf("outcome = %+v, %v", out, err)
	}
	if waited := time.Since(began); waited > 2*time.Second {
		t.Fatalf("outcome took %v with a one-minute poll: the notice path did not fire", waited)
	}
	if starts, restarts := backend.counts(); starts != 1 || restarts != 0 {
		t.Fatalf("backend starts=%d restarts=%d, want 1/0", starts, restarts)
	}
}

// A backend that lost its state (its Server restarted) answers Attach with
// missing; a Worker recovering the execution restarts the model assignment
// as a new generation on the new backend (RUN-EXE-9).
func TestRecoveryRestartsOnAFreshBackend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	now := time.Unix(1_000_000, 0)
	clock := func() time.Time { return now }
	records := storetest.NewMap(clock)

	first := newFakeBackend("first")
	var handler atomic.Pointer[http.Handler]
	h := (&backendhttp.Server{Backend: first, Progress: first.hub}).Handler()
	handler.Store(&h)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { (*handler.Load()).ServeHTTP(w, r) }))
	defer server.Close()
	client := &backendhttp.Client{BaseURL: server.URL}

	a := modelAssignment("e2")
	dead, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("remote", client)}, executor.WorkerOptions{ID: "dead", LeaseDuration: time.Second, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	if err := dead.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	dead.Close()
	// The backend process restarts: a new Server over an empty backend.
	second := newFakeBackend("second")
	h2 := (&backendhttp.Server{Backend: second, Progress: second.hub}).Handler()
	handler.Store(&h2)
	now = now.Add(2 * time.Second)

	live, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("remote", client)}, executor.WorkerOptions{ID: "live", LeaseDuration: time.Second, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	if err := live.RecoverExecution(ctx, a.Key()); err != nil {
		t.Fatal(err)
	}
	second.releaseAll()
	out, err := await(ctx, live, a.Key(), 20*time.Millisecond)
	if err != nil || modelText(out) != "second" {
		t.Fatalf("recovered outcome = %+v, %v", out, err)
	}
	if starts, restarts := second.counts(); starts != 1 || restarts != 1 {
		t.Fatalf("fresh backend starts=%d restarts=%d, want one restart then one start", starts, restarts)
	}
}

// Start's answers follow the backend contract: 400 is the backend's
// definite refusal (an ordinary error); a transport failure or any other
// non-2xx is ErrDispatchUnknown, because the request may have crossed the
// effect boundary.
func TestStartClassification(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name    string
		handler http.HandlerFunc
		unknown bool
	}{
		{"refused", func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "no such model", http.StatusBadRequest) }, false},
		{"gateway error", func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "upstream", http.StatusBadGateway) }, true},
		{"accepted but unknown", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-Twilight-Start", "unknown")
			w.WriteHeader(http.StatusAccepted)
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(tc.handler)
			defer server.Close()
			err := (&backendhttp.Client{BaseURL: server.URL}).Start(ctx, "ref", modelAssignment("e3"))
			if err == nil || errors.Is(err, effect.ErrDispatchUnknown) != tc.unknown {
				t.Fatalf("start = %v, want unknown=%v", err, tc.unknown)
			}
		})
	}
	err := (&backendhttp.Client{BaseURL: "http://127.0.0.1:1"}).Start(ctx, "ref", modelAssignment("e3"))
	if !errors.Is(err, effect.ErrDispatchUnknown) {
		t.Fatalf("start over a dead transport = %v, want unknown", err)
	}
}
