// Package backendhttp is the wire between a Worker and a remote
// ExecutionBackend (CLD-WIR-1): Server exposes one backend over HTTP, Client
// implements executor.ExecutionBackend against such a server. The wire is
// the Go contract as it is: Outcome is a read (/outcome, 204 while
// unsettled) and the backend's notice.Source is a stream (/notices), so no
// request is held open for the length of an execution and the Client keeps
// no state of its own (RUN-EXE-17).
//
// The Server keeps nothing: every endpoint is a call on the backend behind
// it. A server that restarts loses no table, and the backend answers Attach
// with missing for every ref it no longer holds, which is what the Worker's
// recovery expects of a stateless backend (RUN-EXE-3, RUN-EXE-9).
package backendhttp

import (
	"encoding/json"
	"errors"
	"fmt"
	stdhttp "net/http"

	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/executor/notice"
	"github.com/felinics/twilight/agentcore/executor/protocol"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
)

// DefaultMaxBodyBytes bounds a request body when Server.MaxBodyBytes is zero.
const DefaultMaxBodyBytes int64 = 16 << 20

// Server exposes Backend over HTTP. Progress, when set, serves /progress:
// the hub the backend publishes its frames into (RUN-EXE-12). Every endpoint
// accepts POST only.
type Server struct {
	Backend executor.ExecutionBackend
	// Progress, when set, serves /progress: the hub the backend publishes
	// its frames into (RUN-EXE-12).
	Progress     effect.ProgressPort
	MaxBodyBytes int64
}

type refRequest struct {
	Ref string `json:"ref"`
}

type startRequest struct {
	Ref        string            `json:"ref"`
	Assignment effect.Assignment `json:"assignment"`
}

type restartRequest struct {
	Previous   string            `json:"previous"`
	Assignment effect.Assignment `json:"assignment"`
}

type refResponse struct {
	Ref string `json:"ref"`
}

type statusResponse struct {
	Status effect.ExecutionStatus `json:"status"`
}

type validateResponse struct {
	Failure *run.ToolFailure `json:"failure,omitempty"`
}

func (s *Server) validate(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req startRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	failure, err := s.Backend.Validate(r.Context(), req.Assignment)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, validateResponse{Failure: failure})
}

type progressRequest struct {
	Key   effect.AssignmentKey `json:"key"`
	After uint64               `json:"after"`
}

type noticesRequest struct {
	Epoch string `json:"epoch,omitempty"`
	After uint64 `json:"after"`
}

// Notice is the wire form of the backend's settled-Ref notice (notice.Ref):
// read the Outcome with /outcome once it arrives.
type Notice = notice.Ref

// Handler routes the backend protocol.
func (s *Server) Handler() stdhttp.Handler {
	mux := stdhttp.NewServeMux()
	mux.HandleFunc("POST /validate", s.validate)
	mux.HandleFunc("POST /prepare", s.prepare)
	mux.HandleFunc("POST /start", s.start)
	mux.HandleFunc("POST /restart", s.restart)
	mux.HandleFunc("POST /attach", s.attach)
	mux.HandleFunc("POST /status", s.status)
	mux.HandleFunc("POST /outcome", s.outcome)
	mux.HandleFunc("POST /cancel", s.cancel)
	mux.HandleFunc("POST /progress", s.progress)
	mux.HandleFunc("POST /notices", s.noticeStream)
	return mux
}

func (s *Server) prepare(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req startRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	ref, err := s.Backend.Prepare(r.Context(), req.Assignment)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, refResponse{Ref: ref})
}

// start hands the ref to the backend. A definite refusal is 400; an
// uncertain start is 202 like an accepted one, because the Client cannot
// tell the two apart any better than the server can, with the header
// X-Twilight-Start: unknown so the Client returns ErrDispatchUnknown.
func (s *Server) start(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req startRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	err := s.Backend.Start(r.Context(), req.Ref, req.Assignment)
	if err != nil && !errors.Is(err, effect.ErrDispatchUnknown) {
		stdhttp.Error(w, err.Error(), stdhttp.StatusBadRequest)
		return
	}
	if err != nil {
		// The Client maps 202 with this header to ErrDispatchUnknown.
		w.Header().Set("X-Twilight-Start", "unknown")
	}
	w.WriteHeader(stdhttp.StatusAccepted)
}

func (s *Server) restart(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req restartRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	ref, err := s.Backend.Restart(r.Context(), req.Previous, req.Assignment)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, refResponse{Ref: ref})
}

func (s *Server) attach(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req refRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	attachment, err := s.Backend.Attach(r.Context(), req.Ref)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, attachment)
}

func (s *Server) status(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req refRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	status, err := s.Backend.Status(r.Context(), req.Ref)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, statusResponse{Status: status})
}

// outcome is the read: the Outcome once the backend has it, 204 while it
// has none, 404 for a ref it does not hold, 410 for one it will never answer
// (effect.ErrOutcomeUnavailable).
func (s *Server) outcome(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req refRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	out, err := s.Backend.Outcome(r.Context(), req.Ref)
	switch {
	case err == nil:
		writeJSON(w, protocol.EncodeOutcome(out))
	case errors.Is(err, effect.ErrOutcomeNotReady):
		w.WriteHeader(stdhttp.StatusNoContent)
	default:
		writeError(w, err)
	}
}

func (s *Server) cancel(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req refRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	if err := s.Backend.Cancel(r.Context(), req.Ref); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusNoContent)
}

func (s *Server) progress(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req progressRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	if s.Progress == nil {
		sse(w)
		return
	}
	send := sse(w)
	_ = s.Progress.Progress(r.Context(), req.Key, req.After, func(f effect.ProgressFrame) bool { return send(f) })
}

// noticeStream relays the backend's settled Refs (notice.Source) as
// server-sent events; a 410 before the first event is
// effect.ErrSettlementsEvicted. A backend without notices answers an empty
// stream that ends at once, and the Client reads at intervals instead.
func (s *Server) noticeStream(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req noticesRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	source, ok := s.Backend.(notice.Source)
	if !ok {
		sse(w)
		return
	}
	var send func(v any) bool
	err := source.Settled(r.Context(), req.Epoch, req.After, func(n notice.Ref) bool {
		if send == nil {
			send = sse(w)
		}
		return send(n)
	})
	if errors.Is(err, effect.ErrSettlementsEvicted) && send == nil {
		stdhttp.Error(w, err.Error(), stdhttp.StatusGone)
		return
	}
	if send == nil {
		sse(w)
	}
}

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

// writeError maps the backend's definitive answers to statuses the Client
// maps back: 404 is effect.ErrExecutionNotFound (a ref the backend does not
// hold), 410 is effect.ErrOutcomeUnavailable (a ref it will never answer);
// anything else is 500 and reaches the Client as a read failure to retry.
func writeError(w stdhttp.ResponseWriter, err error) {
	status := stdhttp.StatusInternalServerError
	switch {
	case errors.Is(err, effect.ErrExecutionNotFound):
		status = stdhttp.StatusNotFound
	case errors.Is(err, effect.ErrOutcomeUnavailable):
		status = stdhttp.StatusGone
	}
	stdhttp.Error(w, err.Error(), status)
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
