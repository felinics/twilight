package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	stdhttp "net/http"
	"strconv"
	"time"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agent/workspace"
	"github.com/felinics/twilight/agentcore/inbox"
	"github.com/felinics/twilight/agentcore/owner"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/turn"
)

// DefaultMaxBodyBytes bounds a request body when Server gives no cap.
const DefaultMaxBodyBytes int64 = 4 << 20

// Server binds an Application's command face.
type Server struct {
	App *app.Application
	// Options tunes every Session this face opens: the preset comes from
	// the request, the rest from here.
	Options app.SessionOptions
	// MaxWait bounds a command await; zero selects DefaultMaxWait.
	MaxWait      time.Duration
	MaxBodyBytes int64
}

// DefaultMaxWait bounds GET .../commands/{id}?wait=.
const DefaultMaxWait = 30 * time.Second

// Handler routes the command face.
func (s *Server) Handler() stdhttp.Handler {
	mux := stdhttp.NewServeMux()
	mux.HandleFunc("PUT /sessions/{sid}", s.ensure)
	mux.HandleFunc("POST /sessions/{sid}/open", s.open)
	mux.HandleFunc("POST /sessions/{sid}/close", s.close)
	mux.HandleFunc("POST /sessions/{sid}/wake", s.wake)
	mux.HandleFunc("POST /sessions/{sid}/commands", s.enqueue)
	mux.HandleFunc("GET /sessions/{sid}/commands/{id}", s.command)
	mux.HandleFunc("GET /sessions/{sid}/turns", s.turns)
	mux.HandleFunc("GET /sessions/{sid}/turns/{turn}", s.turn)
	mux.HandleFunc("GET /sessions/{sid}/events", s.events)
	mux.HandleFunc("GET /sessions/{sid}/lease", s.lease)
	mux.HandleFunc("GET /sessions/{sid}/workspace", s.workspaceOf)
	mux.HandleFunc("POST /sessions/{sid}/fork", s.fork)
	mux.HandleFunc("POST /workspaces", s.allocateWorkspace)
	return mux
}

func sid(r *stdhttp.Request) session.SessionID { return session.SessionID(r.PathValue("sid")) }

func (s *Server) readJSON(w stdhttp.ResponseWriter, r *stdhttp.Request, out any) bool {
	limit := s.MaxBodyBytes
	if limit <= 0 {
		limit = DefaultMaxBodyBytes
	}
	r.Body = stdhttp.MaxBytesReader(w, r.Body, limit)
	if err := json.NewDecoder(r.Body).Decode(out); err != nil {
		var tooLarge *stdhttp.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, &Error{Status: stdhttp.StatusRequestEntityTooLarge, Code: CodeInvalid, Message: err.Error()})
			return false
		}
		writeError(w, &Error{Status: stdhttp.StatusBadRequest, Code: CodeInvalid, Message: err.Error()})
		return false
	}
	return true
}

func writeJSON(w stdhttp.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

func writeError(w stdhttp.ResponseWriter, e *Error) { writeJSON(w, e.Status, e) }

// fail maps the application's answers to the face's Errors.
func fail(w stdhttp.ResponseWriter, err error) {
	var e *Error
	if errors.As(err, &e) {
		writeError(w, e)
		return
	}
	switch {
	case session.IsCode(err, session.ErrNotFound), errors.Is(err, workspace.ErrNotFound):
		writeError(w, &Error{Status: stdhttp.StatusNotFound, Code: CodeNotFound, Message: err.Error()})
	case session.IsCode(err, session.ErrOwned), errors.Is(err, owner.ErrSessionOpen):
		writeError(w, &Error{Status: stdhttp.StatusConflict, Code: CodeOwned, Message: err.Error()})
	case errors.Is(err, inbox.ErrCommandConflict):
		writeError(w, &Error{Status: stdhttp.StatusConflict, Code: CodeCommandConflict, Message: err.Error()})
	case errors.Is(err, turn.ErrConflict), session.IsCode(err, session.ErrInvalid), errors.Is(err, workspace.ErrExists):
		writeError(w, &Error{Status: stdhttp.StatusConflict, Code: CodeConflict, Message: err.Error()})
	case errors.Is(err, app.ErrNoInbox), errors.Is(err, app.ErrNoWorkspaces), errors.Is(err, app.ErrNoSnapshots):
		writeError(w, &Error{Status: stdhttp.StatusNotImplemented, Code: CodeUnavailable, Message: err.Error()})
	default:
		writeError(w, &Error{Status: stdhttp.StatusInternalServerError, Code: CodeInternal, Message: err.Error()})
	}
}

func (s *Server) ensure(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	if err := s.App.EnsureSession(r.Context(), sid(r)); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusNoContent)
}

func inheritedPolicy(name string) (workspace.InheritedPolicy, error) {
	switch name {
	case "", "share":
		return workspace.InheritShare, nil
	case "none":
		return workspace.InheritNone, nil
	case "allocate":
		return workspace.InheritAllocate, nil
	case "clone":
		return workspace.InheritClone, nil
	case "restore":
		return workspace.InheritRestore, nil
	default:
		return 0, &Error{Status: stdhttp.StatusBadRequest, Code: CodeInvalid, Message: fmt.Sprintf("unknown inherited workspace policy %q", name)}
	}
}

func (s *Server) open(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req OpenRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	id := sid(r)
	if existing, ok := s.App.Opened(id); ok {
		status, err := existing.Status(r.Context())
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, stdhttp.StatusOK, OpenResponse{Recovered: existing.Recovered, Active: status.Active, AlreadyOpen: true})
		return
	}
	if req.Preset == "" {
		// No preset names the activation's (APP-ACT-3): the owner opens the
		// Session as a command reaching it would.
		opened, err := s.App.Activate(context.WithoutCancel(r.Context()), id)
		if errors.Is(err, app.ErrNoActivation) {
			writeError(w, &Error{Status: stdhttp.StatusBadRequest, Code: CodeInvalid, Message: "preset is required"})
			return
		}
		if err != nil {
			fail(w, err)
			return
		}
		status, err := opened.Status(r.Context())
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, stdhttp.StatusOK, OpenResponse{Recovered: opened.Recovered, Active: status.Active})
		return
	}
	preset, err := s.App.PresetRef(req.Preset)
	if err != nil {
		writeError(w, &Error{Status: stdhttp.StatusNotFound, Code: CodeNotFound, Message: err.Error()})
		return
	}
	policy, err := inheritedPolicy(req.InheritedWorkspace)
	if err != nil {
		fail(w, err)
		return
	}
	opts := s.Options
	opts.Preset, opts.InheritedWorkspace, opts.ResumeActive = preset, policy, req.ResumeActive
	// The Session outlives the request: it stays open in this owner until
	// closed or the process ends.
	opened, err := s.App.OpenSession(context.WithoutCancel(r.Context()), id, opts)
	if err != nil {
		fail(w, err)
		return
	}
	status, err := opened.Status(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, OpenResponse{Recovered: opened.Recovered, Active: status.Active})
}

func (s *Server) close(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	opened, ok := s.App.Opened(sid(r))
	if !ok {
		writeError(w, &Error{Status: stdhttp.StatusNotFound, Code: CodeNotOpen, Message: "this owner does not hold the session"})
		return
	}
	if err := opened.Close(r.Context()); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusNoContent)
}

func (s *Server) wake(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	opened, ok := s.App.Opened(sid(r))
	if ok {
		opened.Wake()
	}
	writeJSON(w, stdhttp.StatusOK, WakeResponse{Open: ok})
}

func (s *Server) enqueue(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var c inbox.Command
	if !s.readJSON(w, r, &c) {
		return
	}
	if c.ID == "" || c.Kind == "" {
		writeError(w, &Error{Status: stdhttp.StatusBadRequest, Code: CodeInvalid, Message: "command id and kind are required"})
		return
	}
	entry, err := s.App.Enqueue(r.Context(), sid(r), c)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusAccepted, entry)
}

func (s *Server) command(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	id := inbox.CommandID(r.PathValue("id"))
	ctx := r.Context()
	if wait := r.URL.Query().Get("wait"); wait != "" {
		d, err := time.ParseDuration(wait)
		if err != nil || d < 0 {
			writeError(w, &Error{Status: stdhttp.StatusBadRequest, Code: CodeInvalid, Message: "wait must be a duration"})
			return
		}
		maxWait := s.MaxWait
		if maxWait <= 0 {
			maxWait = DefaultMaxWait
		}
		if d > maxWait {
			d = maxWait
		}
		waitCtx, cancel := context.WithTimeout(ctx, d)
		defer cancel()
		// A timeout is not an error: the entry is answered as it stands.
		if _, err := s.App.AwaitCommand(waitCtx, sid(r), id); err != nil && !errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			if errors.Is(err, app.ErrNoInbox) {
				fail(w, err)
				return
			}
		}
	}
	entry, ok, err := s.App.LookupCommand(ctx, sid(r), id)
	if err != nil {
		fail(w, err)
		return
	}
	if !ok {
		writeError(w, &Error{Status: stdhttp.StatusNotFound, Code: CodeNotFound, Message: "unknown command"})
		return
	}
	writeJSON(w, stdhttp.StatusOK, entry)
}

func (s *Server) turns(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	surface, err := s.App.TurnSurface(r.Context(), sid(r))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, surface)
}

func (s *Server) turn(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	id := sid(r)
	turnID := turn.TurnID(r.PathValue("turn"))
	surface, err := s.App.TurnSurface(r.Context(), id)
	if err != nil {
		fail(w, err)
		return
	}
	view, ok := surface.Turns[turnID]
	if !ok {
		writeError(w, &Error{Status: stdhttp.StatusNotFound, Code: CodeNotFound, Message: "unknown turn"})
		return
	}
	out := TurnResponse{Turn: view}
	if view.Status == turn.TurnCompleted {
		if out.Reply, err = s.App.Reply(r.Context(), turn.TurnRef{SessionID: id, TurnID: turnID}); err != nil {
			fail(w, err)
			return
		}
	}
	writeJSON(w, stdhttp.StatusOK, out)
}

func (s *Server) events(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	id := sid(r)
	var events <-chan app.Event
	if from := r.URL.Query().Get("from"); from != "" {
		n, err := strconv.ParseUint(from, 10, 64)
		if err != nil {
			writeError(w, &Error{Status: stdhttp.StatusBadRequest, Code: CodeInvalid, Message: "from must be a commit sequence"})
			return
		}
		events, err = s.App.EventsFrom(r.Context(), id, session.CommitSeq(n))
		if err != nil {
			fail(w, err)
			return
		}
	} else {
		events = s.App.Events(r.Context(), id)
	}
	send := sse(w)
	for e := range events {
		if !send(EventOf(&e)) {
			return
		}
	}
}

func (s *Server) lease(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	l, held, err := s.App.Lease(r.Context(), sid(r))
	if err != nil {
		fail(w, err)
		return
	}
	out := LeaseResponse{Held: held}
	if held {
		out.Lease = &Lease{Session: l.Session, Epoch: l.Epoch, Owner: l.Owner, UntilUnixMilli: l.UntilUnixMilli}
	}
	writeJSON(w, stdhttp.StatusOK, out)
}

func (s *Server) workspaceOf(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	b, err := s.App.Workspace(r.Context(), sid(r))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, b)
}

func (s *Server) allocateWorkspace(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req AllocateWorkspaceRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	ws, err := s.App.AllocateWorkspace(r.Context(), req.Project, req.Base)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusCreated, ws)
}

func (s *Server) fork(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req ForkRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	if req.Child == "" {
		writeError(w, &Error{Status: stdhttp.StatusBadRequest, Code: CodeInvalid, Message: "child is required"})
		return
	}
	parent := sid(r)
	var header session.SegmentHeader
	var err error
	if req.BeforeTurn != "" {
		header, err = s.App.ForkBeforeTurn(r.Context(), parent, req.BeforeTurn, req.Child)
	} else {
		header, err = s.App.Fork(r.Context(), app.ForkRequest{Parent: parent, At: req.At, Child: req.Child})
	}
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusCreated, header)
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
