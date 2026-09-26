// Package compaction is the reference agent's context policy: it decides
// when the context is too long (Policy), selects the pair-closed suffix a
// compaction keeps (RetainLast), renders the transcript a summary replaces,
// and asks the preset's model for that summary through an effect port
// (Summarizer). It writes nothing: the compaction itself is the chatlog
// package's command.
package compaction

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/felinics/twilight/agent/input"
	"strings"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/frozen"
	"github.com/felinics/twilight/agentcore/run/model/sdkconv"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
)

// CompactorSystemPrompt asks the preset's model for the compaction summary.
const CompactorSystemPrompt = "You are the conversation compactor. Reply with a concise summary of the conversation transcript that preserves facts, decisions, names and open tasks. Reply with the summary text only."

// DefaultRetain is the pair-closed suffix Compact keeps when the Session
// does not configure a window.
const DefaultRetain = 4

// Policy bounds automatic compaction for one Session: AfterEntries
// compacts once the context grows past the count, RetainEntries keeps a
// pair-closed suffix of at most that many entries.
type Policy struct {
	AfterEntries  int
	RetainEntries int
}

// Retain selects the pair-closed suffix of at most RetainEntries entries;
// withinWindow reports whether the whole context already fits the window,
// so compaction has nothing to replace (APP-CKP-1).
func (p Policy) Retain(entries []chatlog.Entry) (retain []chatlog.EntryDigestPair, withinWindow bool) {
	n := p.RetainEntries
	if n <= 0 {
		n = DefaultRetain
	}
	retain = RetainLast(entries, n)
	return retain, len(entries) == 0 || len(retain) >= len(entries)
}

// RetainLast selects a pair-closed suffix of at most n entries: a retained
// tool result pulls in the assistant that issued its call, and an assistant
// with a call whose result is not in the context yet is always kept, so the
// retained set stays valid provider input now and when that result lands
// (APP-CKP-2). The suffix may exceed n by what those rules pull in.
func RetainLast(entries []chatlog.Entry, n int) []chatlog.EntryDigestPair {
	if n <= 0 || len(entries) == 0 {
		return nil
	}
	owner := map[chatlog.CallID]int{} // CallID -> index of the issuing assistant
	settled := map[chatlog.CallID]bool{}
	for i, e := range entries {
		switch {
		case e.Kind == chatlog.EntryAssistant && e.Assistant != nil:
			for _, call := range e.Assistant.CallIDs {
				owner[call] = i
			}
		case e.Kind == chatlog.EntryToolResult && e.ToolResult != nil:
			settled[e.ToolResult.CallID] = true
		}
	}
	start := len(entries) - n
	if start < 0 {
		start = 0
	}
	for call, at := range owner {
		if !settled[call] && at < start {
			start = at
		}
	}
	for changed := true; changed; {
		changed = false
		for i := start; i < len(entries); i++ {
			e := &entries[i]
			if e.Kind != chatlog.EntryToolResult || e.ToolResult == nil {
				continue
			}
			if at, ok := owner[e.ToolResult.CallID]; ok && at < start {
				start = at
				changed = true
			}
		}
	}
	out := make([]chatlog.EntryDigestPair, 0, len(entries)-start)
	for i := start; i < len(entries); i++ {
		out = append(out, entries[i].Pair())
	}
	return out
}

// Summarizer asks a preset's model for the compaction summary. The call is
// an effect like any other and goes through the effect port (APP-CKP-1):
// the request is frozen and dispatched as a model Assignment outside any
// Run, so the Owner holds no model client and a remote executor serves
// it the same way. A crash while it generates writes nothing.
type Summarizer struct {
	// ResolvePreset returns the AgentPreset a PresetRef names.
	ResolvePreset func(turn.PresetRef) (turn.AgentPreset, error)
	// Content stores frozen request bodies.
	Content frozen.Store
	// Executor performs the model effect.
	Executor effect.ExecutionPort
	// Watcher is where the summary's one effect is waited for: the Owner's
	// shared Watcher over Executor, so compaction opens no subscription of
	// its own (RUN-EXE-17).
	Watcher *effect.Watcher
}

// Summarize renders entries and asks the preset's model for the summary.
func (s Summarizer) Summarize(ctx context.Context, sid session.SessionID, presetRef turn.PresetRef, entries []chatlog.Materialized) (string, error) {
	preset, err := s.ResolvePreset(presetRef)
	if err != nil {
		return "", err
	}
	store, err := sdkconv.FreezeModelRequest(sdk.Request{Model: string(preset.Model), Messages: []sdk.Message{
		sdk.SystemMessage(CompactorSystemPrompt),
		sdk.UserMessage(renderTranscript(entries)),
	}})
	if err != nil {
		return "", err
	}
	digest, err := schema.Canonical().DigestRequest(store)
	if err != nil {
		return "", err
	}
	raw, err := schema.Bodies().EncodeRequest(&store, digest)
	if err != nil {
		return "", err
	}
	if err := s.Content.Put(ctx, digest, raw); err != nil {
		return "", err
	}
	a := effect.Assignment{Session: run.Scope(sid), RunID: run.RunID("compact-" + randomHex(8)), StepID: "summary",
		Effect: run.EffectID(randomHex(16)),
		Body:   effect.ModelAssignment{Model: preset.Model, Request: &store, RequestDigest: digest}}
	if err := s.Executor.Dispatch(ctx, a); err != nil {
		return "", err
	}
	// One effect, nothing else to do until it answers: the synchronous form
	// of read-plus-notice (effect.AwaitOutcome), not a held request.
	if s.Watcher == nil {
		return "", errors.New("compaction: Summarizer requires the Owner's Watcher")
	}
	out, err := s.Watcher.Await(ctx, a.Key())
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			_ = s.Executor.Cancel(context.WithoutCancel(ctx), a.Key())
		}
		return "", err
	}
	switch r := out.Result.(type) {
	case effect.ModelSucceeded:
		if strings.TrimSpace(r.Result.Text) == "" {
			return "", errors.New("compaction: compactor returned an empty summary")
		}
		return r.Result.Text, nil
	case effect.ModelFailed:
		return "", fmt.Errorf("compaction: compactor failed: %s: %s", r.Code, r.Message)
	case effect.Cancelled:
		return "", errors.New("compaction: compactor call was cancelled")
	default:
		return "", fmt.Errorf("compaction: compactor delivered %T", out.Result)
	}
}

// renderTranscript flattens materialized entries into the compactor's input.
func renderTranscript(entries []chatlog.Materialized) string {
	var b strings.Builder
	for i := range entries {
		m := &entries[i]
		e := m.Entry
		switch e.Kind {
		case chatlog.EntryInput:
			text, err := input.TextOf(e.Input.Content)
			if err != nil {
				text = e.Input.Content.String()
			}
			fmt.Fprintf(&b, "user: %s\n", text)
		case chatlog.EntryAssistant:
			if text := m.Text(); text != "" {
				fmt.Fprintf(&b, "assistant: %s\n", text)
			}
			for _, call := range m.Calls {
				fmt.Fprintf(&b, "assistant: [calls %s %s]\n", call.Name, call.Input.String())
			}
		case chatlog.EntryToolResult:
			fmt.Fprintf(&b, "tool (%s): %s\n", e.ToolResult.Status, m.Text())
		case chatlog.EntrySummary:
			fmt.Fprintf(&b, "summary: %s\n", m.Text())
		}
	}
	return b.String()
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("compaction: rand: " + err.Error())
	}
	return hex.EncodeToString(b)
}
