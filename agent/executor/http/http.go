package http

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"strings"

	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/executor/protocol"
	"github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
)

// Client is a message-oriented Executor over HTTP. It contains no callback
// endpoint: GetOutcome is a long-polling/read operation and can be retried
// after any network failure.
type Client struct {
	BaseURL string
	HTTP    *stdhttp.Client
}

func (c *Client) client() *stdhttp.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return stdhttp.DefaultClient
}

func (c *Client) Validate(ctx context.Context, a effect.Assignment) (*run.ToolFailure, error) {
	var response struct {
		Failure *run.ToolFailure `json:"failure"`
	}
	if err := c.post(ctx, "/validate", makeAssignmentRequest(a), &response); err != nil {
		return nil, err
	}
	return response.Failure, nil
}

func (c *Client) Dispatch(ctx context.Context, a effect.Assignment) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := c.post(ctx, "/dispatch", makeAssignmentRequest(a), nil)
	var responseErr *responseError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &responseErr) && (responseErr.statusCode == stdhttp.StatusBadRequest || responseErr.statusCode == stdhttp.StatusConflict):
		// The Server's own definite answers: 400 rejects the Assignment,
		// 409 is a conflicting replay or an aborted key. Nothing started
		// (RUN-EXE-3).
		return err
	case errors.As(err, &responseErr) && responseErr.statusCode < stdhttp.StatusInternalServerError:
		// Any other 4xx did not come from the Server's dispatch handler: an
		// intermediary refused before forwarding (429, 408, 404 on a stale
		// route). The Assignment was not judged, so the same Dispatch may
		// succeed later.
		return fmt.Errorf("%w: %w", effect.ErrDispatchRetryable, err)
	case errors.As(err, &responseErr) && responseErr.statusCode == stdhttp.StatusServiceUnavailable:
		// 503 is the server's own "not now": the Worker refused before the
		// barrier for a reason that may pass. An intermediary that did not
		// forward the request answers the same way for the same reason.
		return fmt.Errorf("%w: %w", effect.ErrDispatchRetryable, err)
	}
	// A transport failure or a gateway 5xx can follow acceptance. Preserve
	// the executing target for outcome observation and recovery.
	return fmt.Errorf("%w: %w", effect.ErrDispatchUnknown, err)
}

func (c *Client) Attach(ctx context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	var response effect.Attachment
	if err := c.post(ctx, "/attach", keyRequest{Key: key}, &response); err != nil {
		return effect.Attachment{}, err
	}
	return response, nil
}

// Abort closes the key before any acceptance (RUN-EXE-16); the answer is
// the key's attachment afterwards.
func (c *Client) Abort(ctx context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	var response effect.Attachment
	if err := c.post(ctx, "/abort", keyRequest{Key: key}, &response); err != nil {
		return effect.Attachment{}, err
	}
	return response, nil
}

func (c *Client) GetStatus(ctx context.Context, key effect.AssignmentKey) (effect.ExecutionStatus, error) {
	var response struct {
		Status effect.ExecutionStatus `json:"status"`
	}
	if err := c.post(ctx, "/status", keyRequest{Key: key}, &response); err != nil {
		return effect.ExecutionNotFound, err
	}
	return response.Status, nil
}

// GetOutcome reads the key's Outcome; the Server answers a wait that ran out
// with 204, which is effect.ErrOutcomeNotReady here, so the caller asks
// again (Worker.GetOutcome).
func (c *Client) GetOutcome(ctx context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	var response protocol.OutcomeEnvelope
	found, err := c.postOptional(ctx, "/outcome", keyRequest{Key: key}, &response)
	if err != nil {
		return effect.Outcome{}, err
	}
	if !found {
		return effect.Outcome{}, effect.ErrOutcomeNotReady
	}
	return protocol.DecodeOutcome(&response), nil
}

// Progress is effect.ProgressPort over the server's event stream
// (RUN-EXE-12). The request is cancelled when fn stops or ctx ends.
func (c *Client) Progress(ctx context.Context, key effect.AssignmentKey, after uint64, fn func(effect.ProgressFrame) bool) error {
	return c.stream(ctx, "/progress", progressRequest{Key: key, After: after}, func(data []byte) bool {
		var f effect.ProgressFrame
		if err := json.Unmarshal(data, &f); err != nil {
			return true
		}
		return fn(f)
	})
}

// Settlements is effect.SettlementPort over the server's event stream: one
// subscription per Worker for every key it settles. The request is
// cancelled when fn stops or ctx ends; a 410 is ErrSettlementsEvicted.
func (c *Client) Settlements(ctx context.Context, epoch string, after uint64, fn func(effect.Settlement) bool) error {
	return c.stream(ctx, "/settlements", settlementsRequest{Epoch: epoch, After: after}, func(data []byte) bool {
		var s effect.Settlement
		if err := json.Unmarshal(data, &s); err != nil {
			return true
		}
		return fn(s)
	})
}

// stream posts in to path and hands every `data:` line of the server-sent
// event response to fn until fn returns false, the server ends the stream
// (nil) or ctx ends (ctx.Err()).
func (c *Client) stream(ctx context.Context, path string, in any, fn func(data []byte) bool) error {
	if strings.TrimSpace(c.BaseURL) == "" {
		return errors.New("executor/http: empty executor URL")
	}
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodPost, strings.TrimRight(c.BaseURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		message, _ := io.ReadAll(resp.Body)
		switch resp.StatusCode {
		case stdhttp.StatusNotFound:
			return effect.ErrExecutionNotFound
		case stdhttp.StatusGone:
			return effect.ErrSettlementsEvicted
		}
		return &responseError{status: resp.Status, statusCode: resp.StatusCode, body: strings.TrimSpace(string(message))}
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64<<10), 16<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		if !bytes.HasPrefix(line, []byte("data: ")) {
			continue
		}
		if !fn(bytes.TrimPrefix(line, []byte("data: "))) {
			return nil
		}
	}
	if err := scanner.Err(); err != nil && ctx.Err() == nil {
		return err
	}
	return ctx.Err()
}

func (c *Client) Cancel(ctx context.Context, key effect.AssignmentKey) error {
	return c.post(ctx, "/cancel", keyRequest{Key: key}, nil)
}

// RecoverExecution is effect.Recoverer over HTTP: the Worker takes the
// orphaned record of key back and continues it (RUN-EXE-6). Not part of
// effect.ExecutionPort.
func (c *Client) RecoverExecution(ctx context.Context, key effect.AssignmentKey) error {
	return c.post(ctx, "/recover", keyRequest{Key: key}, nil)
}

// Dispose settles an execution record as Unknown without re-dispatching it,
// so the Owner disposes the Run target on its next read. The caller has
// given the execution up; not part of effect.ExecutionPort.
func (c *Client) Dispose(ctx context.Context, key effect.AssignmentKey) error {
	return c.post(ctx, "/dispose", keyRequest{Key: key}, nil)
}

// Acknowledge is effect.Acknowledger over HTTP (RUN-EXE-13); the Worker
// collects the record on acknowledgement.
func (c *Client) Acknowledge(ctx context.Context, key effect.AssignmentKey) error {
	return c.post(ctx, "/acknowledge", keyRequest{Key: key}, nil)
}

func (c *Client) post(ctx context.Context, path string, in, out any) error {
	_, err := c.postOptional(ctx, path, in, out)
	return err
}

// postOptional is post for an endpoint that may answer 204: found is false
// for that answer and out is left untouched.
func (c *Client) postOptional(ctx context.Context, path string, in, out any) (found bool, err error) {
	if strings.TrimSpace(c.BaseURL) == "" {
		return false, errors.New("executor/http: empty executor URL")
	}
	body, err := json.Marshal(in)
	if err != nil {
		return false, err
	}
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodPost, strings.TrimRight(c.BaseURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client().Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == stdhttp.StatusNoContent {
		return false, nil
	}
	if resp.StatusCode/100 != 2 {
		message, _ := io.ReadAll(resp.Body)
		return false, &responseError{status: resp.Status, statusCode: resp.StatusCode, body: strings.TrimSpace(string(message)), cause: causeOf(resp.StatusCode)}
	}
	if out == nil {
		return true, nil
	}
	return true, json.NewDecoder(resp.Body).Decode(out)
}

// DefaultMaxBodyBytes bounds a request body when Server.MaxBodyBytes is zero.
const DefaultMaxBodyBytes int64 = 16 << 20

// Server exposes a Worker using only request/reply messages. A deployment may
// add authentication, TLS, routing and callback notifications around this
// handler; none of those are part of the Agent Core protocol. Every endpoint
// accepts POST only, and a body larger than MaxBodyBytes (zero takes
// DefaultMaxBodyBytes) is rejected before the Worker sees the request.
type Server struct {
	Worker       *executor.Worker
	MaxBodyBytes int64
}

type responseError struct {
	status     string
	statusCode int
	body       string
	// cause is the effect-level error the status carries, so errors.Is on
	// the client side classifies a remote answer like a local one.
	cause error
}

// causeOf maps the statuses the Server writes for definitive answers back to
// their errors: 404 is effect.ErrExecutionNotFound, 410 is
// effect.ErrOutcomeUnavailable (RUN-EXE-13). Other statuses carry none.
func causeOf(status int) error {
	switch status {
	case stdhttp.StatusNotFound:
		return effect.ErrExecutionNotFound
	case stdhttp.StatusGone:
		return effect.ErrOutcomeUnavailable
	default:
		return nil
	}
}

func (e *responseError) Unwrap() error { return e.cause }

func (e *responseError) Error() string {
	if e.body == "" {
		return "executor/http: " + e.status
	}
	return "executor/http: " + e.status + ": " + e.body
}

type assignmentRequest struct {
	ProtocolVersion uint16            `json:"protocolVersion"`
	Assignment      effect.Assignment `json:"assignment"`
}

type keyRequest struct {
	Key effect.AssignmentKey `json:"key"`
}

// progressRequest subscribes to an execution's frames after a sequence.
type progressRequest struct {
	Key   effect.AssignmentKey `json:"key"`
	After uint64               `json:"after"`
}

// settlementsRequest subscribes to the Worker's settlement notices after a
// sequence of the named incarnation (effect.SettlementPort).
type settlementsRequest struct {
	Epoch string `json:"epoch,omitempty"`
	After uint64 `json:"after"`
}

func (s *Server) Handler() stdhttp.Handler {
	mux := stdhttp.NewServeMux()
	mux.HandleFunc("POST /validate", s.validate)
	mux.HandleFunc("POST /dispatch", s.dispatch)
	mux.HandleFunc("POST /attach", s.attach)
	mux.HandleFunc("POST /abort", s.abort)
	mux.HandleFunc("POST /status", s.status)
	mux.HandleFunc("POST /outcome", s.outcome)
	mux.HandleFunc("POST /cancel", s.cancel)
	mux.HandleFunc("POST /recover", s.recover)
	mux.HandleFunc("POST /dispose", s.dispose)
	mux.HandleFunc("POST /acknowledge", s.acknowledge)
	mux.HandleFunc("POST /progress", s.progress)
	mux.HandleFunc("POST /settlements", s.settlements)
	return mux
}

// sse opens a server-sent event response and returns the writer of one
// `data:` line, which reports false once the client is gone.
func sse(w stdhttp.ResponseWriter) func(v any) bool {
	flusher, _ := w.(stdhttp.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(stdhttp.StatusOK)
	if flusher != nil {
		flusher.Flush()
	}
	return func(v any) bool {
		line, err := json.Marshal(v)
		if err != nil {
			return true
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", line); err != nil {
			return false
		}
		if flusher != nil {
			flusher.Flush()
		}
		return true
	}
}

// progress streams an execution's frames as server-sent events
// (RUN-EXE-12): one `data:` line per frame, flushed as it arrives, until the
// Worker ends the stream or the client goes away.
func (s *Server) progress(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req progressRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	send := sse(w)
	_ = s.Worker.Progress(r.Context(), req.Key, req.After, func(f effect.ProgressFrame) bool { return send(f) })
}

// settlements streams the Worker's settlement notices as server-sent events
// (effect.SettlementPort): one `data:` line per Settlement, until the
// Worker closes or the client goes away. A sequence the Worker has evicted
// is 410, and the client re-reads what it waits on.
func (s *Server) settlements(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req settlementsRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	var send func(v any) bool
	err := s.Worker.Settlements(r.Context(), req.Epoch, req.After, func(st effect.Settlement) bool {
		if send == nil {
			send = sse(w)
		}
		return send(st)
	})
	if errors.Is(err, effect.ErrSettlementsEvicted) && send == nil {
		stdhttp.Error(w, err.Error(), stdhttp.StatusGone)
		return
	}
	if send == nil {
		// Nothing was sent before the stream ended: open it so the client
		// sees a clean end rather than an empty non-SSE body.
		sse(w)
	}
}

func makeAssignmentRequest(a effect.Assignment) assignmentRequest {
	return assignmentRequest{ProtocolVersion: protocol.ProtocolVersion, Assignment: a}
}

func validateAssignmentRequest(req *assignmentRequest) error {
	if req.ProtocolVersion != protocol.ProtocolVersion {
		return fmt.Errorf("executor/http: unsupported protocol version %d", req.ProtocolVersion)
	}
	return nil
}

func (s *Server) validate(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req assignmentRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	if err := validateAssignmentRequest(&req); err != nil {
		writeError(w, err)
		return
	}
	failure, err := s.Worker.Validate(r.Context(), req.Assignment)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, struct {
		Failure *run.ToolFailure `json:"failure,omitempty"`
	}{failure})
}

func (s *Server) dispatch(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req assignmentRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	if err := validateAssignmentRequest(&req); err != nil {
		stdhttp.Error(w, err.Error(), stdhttp.StatusBadRequest)
		return
	}
	// The Worker has persisted acceptance and observes the backend even
	// when the backend's dispatch acknowledgement is uncertain. Every other
	// Dispatch error is a Known answer the client must be able to tell apart
	// from a lost response (RUN-EXE-3): a definite rejection of the
	// Assignment is 400 (a conflicting replay or an aborted key 409), a
	// refusal the Worker itself may lift later is 503. 5xx other than 503
	// never come from here.
	if err := s.Worker.Dispatch(r.Context(), req.Assignment); err != nil && !errors.Is(err, effect.ErrDispatchUnknown) {
		status := stdhttp.StatusBadRequest
		switch {
		case errors.Is(err, store.ErrAssignmentConflict), errors.Is(err, effect.ErrExecutionAborted):
			status = stdhttp.StatusConflict
		case errors.Is(err, effect.ErrDispatchRetryable):
			status = stdhttp.StatusServiceUnavailable
		}
		stdhttp.Error(w, err.Error(), status)
		return
	}
	w.WriteHeader(stdhttp.StatusAccepted)
}

func (s *Server) attach(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req keyRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	attachment, err := s.Worker.Attach(r.Context(), req.Key)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, attachment)
}

func (s *Server) abort(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req keyRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	attachment, err := s.Worker.Abort(r.Context(), req.Key)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, attachment)
}

func (s *Server) status(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req keyRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	status, err := s.Worker.GetStatus(r.Context(), req.Key)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, struct {
		Status effect.ExecutionStatus `json:"status"`
	}{status})
}

func (s *Server) outcome(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req keyRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	outcome, err := s.Worker.GetOutcomeEnvelope(r.Context(), req.Key)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, outcome)
}

func (s *Server) cancel(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req keyRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	if err := s.Worker.Cancel(r.Context(), req.Key); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusAccepted)
}

func (s *Server) recover(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req keyRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	if err := s.Worker.RecoverExecution(r.Context(), req.Key); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusAccepted)
}

func (s *Server) dispose(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req keyRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	if err := s.Worker.Dispose(r.Context(), req.Key); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusAccepted)
}

func (s *Server) acknowledge(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req keyRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	if err := s.Worker.Acknowledge(r.Context(), req.Key); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusAccepted)
}

// readJSON decodes the request body within the Server's size bound: a body
// past it is 413, any other decoding failure 400.
func (s *Server) readJSON(w stdhttp.ResponseWriter, r *stdhttp.Request, out any) bool {
	limit := s.MaxBodyBytes
	if limit <= 0 {
		limit = DefaultMaxBodyBytes
	}
	r.Body = stdhttp.MaxBytesReader(w, r.Body, limit)
	if err := json.NewDecoder(r.Body).Decode(out); err != nil {
		var tooLarge *stdhttp.MaxBytesError
		if errors.As(err, &tooLarge) {
			stdhttp.Error(w, err.Error(), stdhttp.StatusRequestEntityTooLarge)
			return false
		}
		stdhttp.Error(w, err.Error(), stdhttp.StatusBadRequest)
		return false
	}
	return true
}

func writeJSON(w stdhttp.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(stdhttp.StatusOK)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w stdhttp.ResponseWriter, err error) {
	status := stdhttp.StatusInternalServerError
	if errors.Is(err, effect.ErrExecutionNotFound) {
		status = stdhttp.StatusNotFound
	}
	if errors.Is(err, effect.ErrOutcomeUnavailable) {
		// A definitive answer: nothing will ever be read for the key.
		status = stdhttp.StatusGone
	}
	if errors.Is(err, store.ErrAssignmentConflict) || errors.Is(err, store.ErrLeaseLost) || errors.Is(err, store.ErrStateConflict) {
		status = stdhttp.StatusConflict
	}
	if errors.Is(err, effect.ErrOutcomeNotReady) {
		// 204 responses carry no body; writing one violates the HTTP contract.
		w.WriteHeader(stdhttp.StatusNoContent)
		return
	}
	stdhttp.Error(w, err.Error(), status)
}

var (
	_ effect.ExecutionPort  = (*Client)(nil)
	_ effect.Acknowledger   = (*Client)(nil)
	_ effect.SettlementPort = (*Client)(nil)
)
