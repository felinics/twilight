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
	"net/url"
	"strings"
	"time"

	"github.com/felinics/twilight/agent/workspace"
	"github.com/felinics/twilight/agentcore/inbox"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/turn"
)

// Client drives an owner's command face.
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

func (c *Client) url(path string, query url.Values) string {
	u := strings.TrimRight(c.BaseURL, "/") + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	return u
}

// do sends one request and decodes a JSON answer into out (nil for none);
// an error response comes back as *Error.
func (c *Client) do(ctx context.Context, method, path string, query url.Values, in, out any) error {
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	req, err := stdhttp.NewRequestWithContext(ctx, method, c.url(path, query), body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.client().Do(req)
	if err != nil {
		return fmt.Errorf("owner http: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		e := &Error{Status: resp.StatusCode, Code: CodeInternal}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, DefaultMaxBodyBytes))
		if json.Unmarshal(raw, e) != nil || e.Code == "" {
			e.Code, e.Message = CodeInternal, strings.TrimSpace(string(raw))
		}
		e.Status = resp.StatusCode
		return e
	}
	if out == nil || resp.StatusCode == stdhttp.StatusNoContent {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func sessionPath(sid session.SessionID, rest string) string {
	return "/sessions/" + url.PathEscape(string(sid)) + rest
}

// Ensure creates the Session when it does not exist.
func (c *Client) Ensure(ctx context.Context, sid session.SessionID) error {
	return c.do(ctx, stdhttp.MethodPut, sessionPath(sid, ""), nil, nil, nil)
}

// Open opens the Session in the owner behind BaseURL.
func (c *Client) Open(ctx context.Context, sid session.SessionID, req OpenRequest) (OpenResponse, error) {
	var out OpenResponse
	err := c.do(ctx, stdhttp.MethodPost, sessionPath(sid, "/open"), nil, req, &out)
	return out, err
}

// Close releases the Session from the owner behind BaseURL.
func (c *Client) Close(ctx context.Context, sid session.SessionID) error {
	return c.do(ctx, stdhttp.MethodPost, sessionPath(sid, "/close"), nil, struct{}{}, nil)
}

// Wake asks the owner to read the Session's inbox now; Open reports whether
// it holds the Session.
func (c *Client) Wake(ctx context.Context, sid session.SessionID) (bool, error) {
	var out WakeResponse
	err := c.do(ctx, stdhttp.MethodPost, sessionPath(sid, "/wake"), nil, struct{}{}, &out)
	return out.Open, err
}

// Enqueue leaves a command in the Session's inbox (APP-INB-1).
func (c *Client) Enqueue(ctx context.Context, sid session.SessionID, cmd inbox.Command) (inbox.Entry, error) {
	var out inbox.Entry
	err := c.do(ctx, stdhttp.MethodPost, sessionPath(sid, "/commands"), nil, cmd, &out)
	return out, err
}

// Command reads a command's entry, waiting up to wait for its resolution
// when wait is positive.
func (c *Client) Command(ctx context.Context, sid session.SessionID, id inbox.CommandID, wait time.Duration) (inbox.Entry, error) {
	var query url.Values
	if wait > 0 {
		query = url.Values{"wait": {wait.String()}}
	}
	var out inbox.Entry
	err := c.do(ctx, stdhttp.MethodGet, sessionPath(sid, "/commands/"+url.PathEscape(string(id))), query, nil, &out)
	return out, err
}

// Await blocks until the command is resolved or ctx ends, re-asking the
// owner in bounded waits.
func (c *Client) Await(ctx context.Context, sid session.SessionID, id inbox.CommandID) (inbox.Result, error) {
	for {
		e, err := c.Command(ctx, sid, id, DefaultMaxWait)
		if err != nil {
			return inbox.Result{}, err
		}
		if !e.Pending() {
			return *e.Result, nil
		}
		if ctx.Err() != nil {
			return inbox.Result{}, ctx.Err()
		}
	}
}

// Turns is the Session's Turn surface.
func (c *Client) Turns(ctx context.Context, sid session.SessionID) (turn.TurnSurface, error) {
	var out turn.TurnSurface
	err := c.do(ctx, stdhttp.MethodGet, sessionPath(sid, "/turns"), nil, nil, &out)
	return out, err
}

// Turn is one Turn's view and reply.
func (c *Client) Turn(ctx context.Context, sid session.SessionID, id turn.TurnID) (TurnResponse, error) {
	var out TurnResponse
	err := c.do(ctx, stdhttp.MethodGet, sessionPath(sid, "/turns/"+url.PathEscape(string(id))), nil, nil, &out)
	return out, err
}

// Lease is the Session's writer lease (SES-OWN-5).
func (c *Client) Lease(ctx context.Context, sid session.SessionID) (LeaseResponse, error) {
	var out LeaseResponse
	err := c.do(ctx, stdhttp.MethodGet, sessionPath(sid, "/lease"), nil, nil, &out)
	return out, err
}

// Workspace is the Session's workspace binding.
func (c *Client) Workspace(ctx context.Context, sid session.SessionID) (workspace.Binding, error) {
	var out workspace.Binding
	err := c.do(ctx, stdhttp.MethodGet, sessionPath(sid, "/workspace"), nil, nil, &out)
	return out, err
}

// AllocateWorkspace creates a Workspace record.
func (c *Client) AllocateWorkspace(ctx context.Context, req AllocateWorkspaceRequest) (workspace.Workspace, error) {
	var out workspace.Workspace
	err := c.do(ctx, stdhttp.MethodPost, "/workspaces", nil, req, &out)
	return out, err
}

// Fork forks the Session.
func (c *Client) Fork(ctx context.Context, parent session.SessionID, req ForkRequest) (session.SegmentHeader, error) {
	var out session.SegmentHeader
	err := c.do(ctx, stdhttp.MethodPost, sessionPath(parent, "/fork"), nil, req, &out)
	return out, err
}

// Events streams the Session's events to fn until fn returns false, ctx
// ends or the stream closes; from > 0 starts from that commit sequence
// (OBS-2).
func (c *Client) Events(ctx context.Context, sid session.SessionID, from session.CommitSeq, fn func(Event) bool) error {
	var query url.Values
	if from > 0 {
		query = url.Values{"from": {fmt.Sprint(uint64(from))}}
	}
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, c.url(sessionPath(sid, "/events"), query), stdhttp.NoBody)
	if err != nil {
		return err
	}
	resp, err := c.client().Do(req)
	if err != nil {
		return fmt.Errorf("owner http: events: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != stdhttp.StatusOK {
		e := &Error{Status: resp.StatusCode, Code: CodeInternal}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, DefaultMaxBodyBytes))
		_ = json.Unmarshal(raw, e)
		e.Status = resp.StatusCode
		return e
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64<<10), int(DefaultMaxBodyBytes))
	for scanner.Scan() {
		line := scanner.Bytes()
		if !bytes.HasPrefix(line, []byte("data: ")) {
			continue
		}
		var e Event
		if err := json.Unmarshal(bytes.TrimPrefix(line, []byte("data: ")), &e); err != nil {
			return fmt.Errorf("owner http: events: %w", err)
		}
		if !fn(e) {
			return nil
		}
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, context.Canceled) && ctx.Err() == nil {
		return err
	}
	return nil
}
