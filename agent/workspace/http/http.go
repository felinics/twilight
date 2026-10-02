// Package http is the HTTP binding of the workspace control face a tool
// backend exposes beside its Backend protocol (APP-WSP-7, CLD-TOL-1): the
// owner asks the process that holds the environments to take a Snapshot.
// Server serves a workspace.Snapshotter; Client is one.
package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"net/url"
	"strings"

	"github.com/felinics/twilight/agent/environment"
	"github.com/felinics/twilight/agent/workspace"
)

// DefaultMaxBodyBytes bounds a request body when Server gives no cap.
const DefaultMaxBodyBytes int64 = 1 << 20

// Server serves the workspace control face.
type Server struct {
	Snapshots    workspace.Snapshotter
	MaxBodyBytes int64
}

// wireError is the body of every error response.
type wireError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Handler routes the control face: POST /workspaces/{id}/snapshot.
func (s *Server) Handler() stdhttp.Handler {
	mux := stdhttp.NewServeMux()
	mux.HandleFunc("POST /workspaces/{id}/snapshot", s.snapshot)
	return mux
}

func (s *Server) snapshot(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	id := workspace.ID(r.PathValue("id"))
	if id == "" {
		writeError(w, stdhttp.StatusBadRequest, "invalid_request", "workspace id is required")
		return
	}
	snap, err := s.Snapshots.Snapshot(r.Context(), id)
	if err != nil {
		switch {
		case errors.Is(err, workspace.ErrNotFound):
			writeError(w, stdhttp.StatusNotFound, "not_found", err.Error())
		case errors.Is(err, workspace.ErrNothingToSnapshot):
			writeError(w, stdhttp.StatusConflict, "nothing_to_snapshot", err.Error())
		case errors.Is(err, environment.ErrUnsupported):
			writeError(w, stdhttp.StatusNotImplemented, "unsupported", err.Error())
		default:
			writeError(w, stdhttp.StatusInternalServerError, "internal", err.Error())
		}
		return
	}
	writeJSON(w, stdhttp.StatusOK, snap)
}

func writeJSON(w stdhttp.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w stdhttp.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, wireError{Code: code, Message: message})
}

// Client is a workspace.Snapshotter over a Server.
type Client struct {
	BaseURL string
	HTTP    *stdhttp.Client
}

var _ workspace.Snapshotter = (*Client)(nil)

func (c *Client) client() *stdhttp.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return stdhttp.DefaultClient
}

// Snapshot asks the server for a Snapshot of id; the server's sentinel
// answers come back as the same errors.
func (c *Client) Snapshot(ctx context.Context, id workspace.ID) (workspace.Snapshot, error) {
	if id == "" {
		return workspace.Snapshot{}, errors.New("workspace/http: snapshot requires a workspace id")
	}
	target := strings.TrimRight(c.BaseURL, "/") + "/workspaces/" + url.PathEscape(string(id)) + "/snapshot"
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodPost, target, stdhttp.NoBody) //nolint:gosec // G704: BaseURL is the deployment's configured tool backend; id is path-escaped
	if err != nil {
		return workspace.Snapshot{}, err
	}
	resp, err := c.client().Do(req) //nolint:gosec // G704: see above
	if err != nil {
		return workspace.Snapshot{}, fmt.Errorf("workspace/http: snapshot %s: %w", id, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, DefaultMaxBodyBytes))
	if err != nil {
		return workspace.Snapshot{}, err
	}
	if resp.StatusCode != stdhttp.StatusOK {
		var we wireError
		_ = json.Unmarshal(body, &we)
		switch resp.StatusCode {
		case stdhttp.StatusNotFound:
			return workspace.Snapshot{}, fmt.Errorf("%w: %s", workspace.ErrNotFound, we.Message)
		case stdhttp.StatusConflict:
			return workspace.Snapshot{}, fmt.Errorf("%w: %s", workspace.ErrNothingToSnapshot, we.Message)
		case stdhttp.StatusNotImplemented:
			return workspace.Snapshot{}, fmt.Errorf("%w: %s", environment.ErrUnsupported, we.Message)
		default:
			return workspace.Snapshot{}, fmt.Errorf("workspace/http: snapshot %s: status %d: %s", id, resp.StatusCode, we.Message)
		}
	}
	var snap workspace.Snapshot
	if err := json.Unmarshal(body, &snap); err != nil {
		return workspace.Snapshot{}, fmt.Errorf("workspace/http: snapshot %s: %w", id, err)
	}
	return snap, nil
}
