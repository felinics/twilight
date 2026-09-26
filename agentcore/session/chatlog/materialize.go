package chatlog

import (
	"context"
	"fmt"

	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/run/schema"
)

// ContentResolver resolves the frozen bodies structural entries name by
// digest (RUN-WIR-4). agent/session/run.Content is the first-party
// implementation; the projections never call it (CHT-MAT-1).
type ContentResolver interface {
	ModelResult(context.Context, es.Digest) (model.ModelResult, error)
	ToolOutput(context.Context, es.Digest) (run.CanonicalJSON, error)
	ToolResponse(context.Context, es.Digest) (run.CanonicalJSON, error)
}

// Call is one tool call of a materialized assistant: the Run's CallID paired
// with the provider's identifiers and arguments from the frozen result.
type Call struct {
	CallID         CallID
	ProviderCallID string
	Name           string
	Input          run.CanonicalJSON
}

// Materialized is one entry with its frozen body resolved: Result and Calls
// for an assistant, Output for a successful tool result. Input, failed tool
// results and summaries carry their content in the entry itself.
type Materialized struct {
	Entry  *Entry
	Result *model.ModelResult
	Calls  []Call
	Output *run.CanonicalJSON
}

// Text is the rendering of the entry's main content: the assistant text, the
// tool output or failure, the summary text. Inputs are rendered by the layer
// that knows their payload shape (DEC-INP-1).
func (m *Materialized) Text() string {
	switch m.Entry.Kind {
	case EntryAssistant:
		if m.Result != nil {
			return m.Result.Text
		}
	case EntryToolResult:
		if m.Output != nil {
			return m.Output.String()
		}
		return m.Entry.ToolResult.FailureText()
	case EntrySummary:
		return PartsText(m.Entry.Summary.Parts)
	}
	return ""
}

// Materializer resolves entries through a ContentResolver, reading each
// digest at most once per Materializer (CHT-MAT-1).
type Materializer struct {
	content ContentResolver
	results map[es.Digest]*model.ModelResult
	outputs map[es.Digest]*run.CanonicalJSON
}

func NewMaterializer(content ContentResolver) *Materializer {
	return &Materializer{content: content, results: map[es.Digest]*model.ModelResult{}, outputs: map[es.Digest]*run.CanonicalJSON{}}
}

// Entries materializes every entry in order.
func (m *Materializer) Entries(ctx context.Context, entries []Entry) ([]Materialized, error) {
	out := make([]Materialized, 0, len(entries))
	for i := range entries {
		e, err := m.Entry(ctx, &entries[i])
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

// Entry materializes one entry.
func (m *Materializer) Entry(ctx context.Context, e *Entry) (Materialized, error) {
	out := Materialized{Entry: e}
	switch e.Kind {
	case EntryAssistant:
		if e.Assistant == nil {
			return out, fmt.Errorf("chatlog: assistant entry %s has no assistant", e.ID)
		}
		result, err := m.result(ctx, e.Assistant.ResultDigest)
		if err != nil {
			return out, err
		}
		calls, err := pairCalls(e.Assistant, result)
		if err != nil {
			return out, err
		}
		out.Result, out.Calls = result, calls
	case EntryToolResult:
		r := e.ToolResult
		if r == nil {
			return out, fmt.Errorf("chatlog: tool result entry %s has no result", e.ID)
		}
		if r.Status != ToolSuccess {
			return out, nil
		}
		output, err := m.output(ctx, r)
		if err != nil {
			return out, err
		}
		out.Output = output
	}
	return out, nil
}

func (m *Materializer) result(ctx context.Context, digest es.Digest) (*model.ModelResult, error) {
	if r, ok := m.results[digest]; ok {
		return r, nil
	}
	if m.content == nil {
		return nil, fmt.Errorf("chatlog: no content resolver for model result %s", digest)
	}
	r, err := m.content.ModelResult(ctx, digest)
	if err != nil {
		return nil, err
	}
	m.results[digest] = &r
	return &r, nil
}

func (m *Materializer) output(ctx context.Context, r *ToolResult) (*run.CanonicalJSON, error) {
	if o, ok := m.outputs[r.OutputDigest]; ok {
		return o, nil
	}
	if m.content == nil {
		return nil, fmt.Errorf("chatlog: no content resolver for tool result %s", r.ID)
	}
	var (
		o   run.CanonicalJSON
		err error
	)
	switch r.Source {
	case SourceToolOutput:
		o, err = m.content.ToolOutput(ctx, r.OutputDigest)
	case SourceToolResponse:
		o, err = m.content.ToolResponse(ctx, r.OutputDigest)
	default:
		return nil, fmt.Errorf("chatlog: tool result %s names no body", r.ID)
	}
	if err != nil {
		return nil, err
	}
	m.outputs[r.OutputDigest] = &o
	return &o, nil
}

// pairCalls zips the Run-assigned CallIDs with the result's ToolCalls. A
// result issued without a ToolStepOpened in the entry (no CallIDs) derives
// them the way the protocol does (RUN-WIR-4 identity table).
func pairCalls(a *Assistant, result *model.ModelResult) ([]Call, error) {
	if len(result.ToolCalls) == 0 {
		return nil, nil
	}
	if len(a.CallIDs) != 0 && len(a.CallIDs) != len(result.ToolCalls) {
		return nil, fmt.Errorf("chatlog: assistant %s has %d call ids for %d tool calls", a.ID, len(a.CallIDs), len(result.ToolCalls))
	}
	calls := make([]Call, len(result.ToolCalls))
	for i, tc := range result.ToolCalls {
		var id CallID
		if len(a.CallIDs) != 0 {
			id = a.CallIDs[i]
		} else {
			id = CallID(schema.Identity().DeriveCallID(a.StepID, i))
		}
		calls[i] = Call{CallID: id, ProviderCallID: tc.ToolCallID, Name: tc.ToolName, Input: tc.Input.Canonical()}
	}
	return calls, nil
}
