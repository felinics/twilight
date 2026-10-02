package backendhttp

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
	"github.com/felinics/twilight/agentcore/executor/notice"
	"github.com/felinics/twilight/agentcore/executor/protocol"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
)

// Client is executor.ExecutionBackend and notice.Source over a Server. It
// keeps no state: Outcome is one read of /outcome, Settled one stream of
// /notices, and the Worker that drives it owns the waiting (RUN-EXE-17).
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
	var out validateResponse
	if _, err := c.post(ctx, "/validate", startRequest{Assignment: a}, &out); err != nil {
		return nil, err
	}
	return out.Failure, nil
}

func (c *Client) Prepare(ctx context.Context, a effect.Assignment) (string, error) {
	var out refResponse
	if _, err := c.post(ctx, "/prepare", startRequest{Assignment: a}, &out); err != nil {
		return "", err
	}
	return out.Ref, nil
}

// Start hands ref to the Server. A 400 is the backend's definite refusal; a
// 202 marked unknown, a transport failure or any other status may have
// crossed the effect boundary and is effect.ErrDispatchUnknown.
func (c *Client) Start(ctx context.Context, ref string, a effect.Assignment) error {
	resp, err := c.do(ctx, "/start", startRequest{Ref: ref, Assignment: a})
	if err != nil {
		return fmt.Errorf("%w: %w", effect.ErrDispatchUnknown, err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == stdhttp.StatusBadRequest:
		message, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("backendhttp: start refused: %s", strings.TrimSpace(string(message)))
	case resp.StatusCode/100 == 2:
		if resp.Header.Get("X-Twilight-Start") == "unknown" {
			return effect.ErrDispatchUnknown
		}
		return nil
	default:
		message, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%w: backendhttp: start answered %s: %s", effect.ErrDispatchUnknown, resp.Status, strings.TrimSpace(string(message)))
	}
}

func (c *Client) Restart(ctx context.Context, previous string, a effect.Assignment) (string, error) {
	var out refResponse
	if _, err := c.post(ctx, "/restart", restartRequest{Previous: previous, Assignment: a}, &out); err != nil {
		return "", err
	}
	return out.Ref, nil
}

// Attach forwards to the Server. A transport failure is an error, never an
// observation: the Worker keeps the lease and asks again (RUN-EXE-3).
func (c *Client) Attach(ctx context.Context, ref string) (effect.Attachment, error) {
	var out effect.Attachment
	if _, err := c.post(ctx, "/attach", refRequest{Ref: ref}, &out); err != nil {
		return effect.Attachment{}, err
	}
	return out, nil
}

func (c *Client) Status(ctx context.Context, ref string) (effect.ExecutionStatus, error) {
	var out statusResponse
	if _, err := c.post(ctx, "/status", refRequest{Ref: ref}, &out); err != nil {
		return effect.ExecutionNotFound, err
	}
	return out.Status, nil
}

// Outcome is the read: the Outcome once the backend has it,
// effect.ErrOutcomeNotReady on a 204.
func (c *Client) Outcome(ctx context.Context, ref string) (effect.Outcome, error) {
	var envelope protocol.OutcomeEnvelope
	found, err := c.post(ctx, "/outcome", refRequest{Ref: ref}, &envelope)
	if err != nil {
		return effect.Outcome{}, err
	}
	if !found {
		return effect.Outcome{}, effect.ErrOutcomeNotReady
	}
	return protocol.DecodeOutcome(&envelope), nil
}

func (c *Client) Cancel(ctx context.Context, ref string) error {
	_, err := c.post(ctx, "/cancel", refRequest{Ref: ref}, nil)
	return err
}

// Progress is effect.ProgressPort over the Server's /progress, so a Worker
// routing to this Client relays the backend's frames (RUN-EXE-12).
func (c *Client) Progress(ctx context.Context, key effect.AssignmentKey, after uint64, fn func(effect.ProgressFrame) bool) error {
	return c.streamLines(ctx, "/progress", progressRequest{Key: key, After: after}, func(data []byte) bool {
		var f effect.ProgressFrame
		if err := json.Unmarshal(data, &f); err != nil {
			return true
		}
		return fn(f)
	})
}

// Settled streams the backend's settled Refs (notice.Source); a 410 before
// the first event is effect.ErrSettlementsEvicted, and a stream that ends
// tells the Worker to read at intervals until it reconnects.
func (c *Client) Settled(ctx context.Context, epoch string, after uint64, fn func(notice.Ref) bool) error {
	return c.streamLines(ctx, "/notices", noticesRequest{Epoch: epoch, After: after}, func(data []byte) bool {
		var n notice.Ref
		if err := json.Unmarshal(data, &n); err != nil {
			return true
		}
		return fn(n)
	})
}

func (c *Client) post(ctx context.Context, path string, in, out any) (found bool, err error) {
	resp, err := c.do(ctx, path, in)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == stdhttp.StatusNoContent {
		return false, nil
	}
	if resp.StatusCode/100 != 2 {
		message, _ := io.ReadAll(resp.Body)
		switch resp.StatusCode {
		case stdhttp.StatusNotFound:
			return false, effect.ErrExecutionNotFound
		case stdhttp.StatusGone:
			return false, effect.ErrOutcomeUnavailable
		}
		return false, fmt.Errorf("backendhttp: %s answered %s: %s", path, resp.Status, strings.TrimSpace(string(message)))
	}
	if out == nil {
		return true, nil
	}
	return true, json.NewDecoder(resp.Body).Decode(out)
}

func (c *Client) do(ctx context.Context, path string, in any) (*stdhttp.Response, error) {
	if strings.TrimSpace(c.BaseURL) == "" {
		return nil, errors.New("backendhttp: empty backend URL")
	}
	body, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	// BaseURL is deployment configuration (the backend this Client was
	// built for), not request input; path is one of this file's literals.
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodPost, strings.TrimRight(c.BaseURL, "/")+path, bytes.NewReader(body)) //nolint:gosec // G704: see above
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.client().Do(req) //nolint:gosec // G704: see above
}

// streamLines posts in to path and hands every `data:` line of the
// server-sent event response to fn until fn stops, the stream ends (nil) or
// ctx ends; a 410 is errNoticesEvicted. onOpen, when set, runs once the
// Server has accepted the subscription.
func (c *Client) streamLines(ctx context.Context, path string, in any, fn func(data []byte) bool) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	resp, err := c.do(ctx, path, in)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		message, _ := io.ReadAll(resp.Body)
		if resp.StatusCode == stdhttp.StatusGone {
			return effect.ErrSettlementsEvicted
		}
		return fmt.Errorf("backendhttp: %s answered %s: %s", path, resp.Status, strings.TrimSpace(string(message)))
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

var (
	_ executor.ExecutionBackend = (*Client)(nil)
	_ effect.ProgressPort       = (*Client)(nil)
	_ notice.Source             = (*Client)(nil)
)
