// Package chatlog is the first-party conversation module
// (docs/design/agent-session-chatlog.md). It owns the conversation's own
// facts -- the input lifecycle, summaries, compactions and out-of-band
// result supersession -- and projects the conversation (Surface) and the
// model-facing context (Context) from those facts together with the Run
// facts of agent/run. Assistant and tool_result entries are projections of
// ModelStepCompleted, ToolCallCompleted, ToolCallAnswered and ToolCallFailed:
// the module writes no second copy of a model or tool outcome. Turn lifecycle
// belongs to agent/turn; execution facts to agent/run.
package chatlog

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/attempt"
	"github.com/felinics/twilight/agentcore/session/extension"
	runmod "github.com/felinics/twilight/agentcore/session/run"
)

const ModuleID extension.ModuleID = "chatlog"

type (
	TurnID       string
	InputID      string
	AssistantID  string
	ToolResultID string
	SummaryID    string
	CallID       string
	CompactionID string
)

// EventTypes (CHT-EVT-1): the conversation's own facts. Inputs are written by
// the host and the Coordinator, compactions by the host's compaction
// (CHT-EVT-3), tool_result_superseded by the Application after an
// out-of-band verification (CHT-ENT-2).
const (
	TypeInputSubmitted        session.EventType = "twilight/chatlog/input_submitted"
	TypeInputDelivered        session.EventType = "twilight/chatlog/input_delivered"
	TypeInputWithdrawn        session.EventType = "twilight/chatlog/input_withdrawn"
	TypeInputRejected         session.EventType = "twilight/chatlog/input_rejected"
	TypeToolResultSuperseded  session.EventType = "twilight/chatlog/tool_result_superseded"
	TypeSummary               session.EventType = "twilight/chatlog/summary"
	TypeCompactionCreated     session.EventType = "twilight/chatlog/compaction_created"
	TypeCompactionInvalidated session.EventType = "twilight/chatlog/compaction_invalidated"
)

// Digest domains of the projected entries (CHT-COD-3). They are not event
// types: an assistant or tool_result entry is folded from Run facts.
const (
	assistantDomain  = "twilight/chatlog/assistant"
	toolResultDomain = "twilight/chatlog/tool_result"
)

// --- parts --------------------------------------------------------------------

// PartKind is the discriminator of summary parts. Model output is not parts:
// an assistant entry names its frozen ModelResult by digest.
type PartKind string

const (
	PartText      PartKind = "twilight/chatlog/text"
	PartReference PartKind = "twilight/chatlog/reference"
)

type Part interface{ PartKind() PartKind }

type TextPart struct{ Text string }
type ReferencePart struct {
	BindingID artifact.BindingID
	Name      string
}

func (TextPart) PartKind() PartKind      { return PartText }
func (ReferencePart) PartKind() PartKind { return PartReference }

// Parts is the ordered part list with its discriminated-union wire.
type Parts []Part

type partWire struct {
	Kind      PartKind `json:"kind"`
	Text      string   `json:"text,omitempty"`
	Name      string   `json:"name,omitempty"`
	BindingID string   `json:"bindingId,omitempty"`
}

func (ps Parts) MarshalJSON() ([]byte, error) {
	wires := make([]partWire, 0, len(ps))
	for i, p := range ps {
		w, err := encodePart(p)
		if err != nil {
			return nil, fmt.Errorf("part %d: %w", i, err)
		}
		wires = append(wires, w)
	}
	return json.Marshal(wires)
}

func (ps *Parts) UnmarshalJSON(raw []byte) error {
	val, err := jsonstable.Parse(raw)
	if err != nil {
		return err
	}
	var wires []partWire
	if err := extension.StrictDecode(val, &wires); err != nil {
		return err
	}
	out := make(Parts, 0, len(wires))
	for i := range wires {
		p, err := decodePart(&wires[i])
		if err != nil {
			return fmt.Errorf("part %d: %w", i, err)
		}
		out = append(out, p)
	}
	*ps = out
	return nil
}

func encodePart(p Part) (partWire, error) {
	switch v := p.(type) {
	case TextPart:
		return partWire{Kind: PartText, Text: v.Text}, nil
	case ReferencePart:
		if v.BindingID == "" {
			return partWire{}, errors.New("reference part requires bindingId")
		}
		return partWire{Kind: PartReference, BindingID: string(v.BindingID), Name: v.Name}, nil
	default:
		return partWire{}, fmt.Errorf("unknown part %T", p)
	}
}

func decodePart(w *partWire) (Part, error) {
	switch w.Kind {
	case PartText:
		if w.BindingID != "" || w.Name != "" {
			return nil, errors.New("text part carries foreign fields")
		}
		return TextPart{Text: w.Text}, nil
	case PartReference:
		if w.BindingID == "" || w.Text != "" {
			return nil, errors.New("malformed reference part")
		}
		return ReferencePart{BindingID: artifact.BindingID(w.BindingID), Name: w.Name}, nil
	default:
		return nil, fmt.Errorf("unknown part kind %q", w.Kind)
	}
}

// PartsText renders the text of summary parts; references are named, not
// materialized.
func PartsText(parts Parts) string {
	var out string
	for _, part := range parts {
		switch v := part.(type) {
		case TextPart:
			out += v.Text
		case ReferencePart:
			out += "[attachment " + v.Name + "]"
		}
	}
	return out
}

// --- entries ------------------------------------------------------------------

type ToolResultStatus string

const (
	ToolSuccess ToolResultStatus = "success"
	ToolError   ToolResultStatus = "error"
	ToolUnknown ToolResultStatus = "unknown"
)

// ToolResultSource says which frozen body a successful tool result names:
// the tool's own output (ToolCallCompleted) or an external answer
// (ToolCallAnswered). A failed result names no body.
type ToolResultSource string

const (
	SourceToolOutput   ToolResultSource = "tool_output"
	SourceToolResponse ToolResultSource = "tool_response"
)

type Input struct {
	ID      InputID          `json:"id"`
	TurnID  TurnID           `json:"turnId,omitempty"`
	Content jsonstable.Value `json:"content"`
	Digest  es.Digest        `json:"digest"`
}

// Assistant is the structural projection of one ModelStepCompleted
// (CHT-ENT-1): the identities of the step and its Turn, the digest that names
// the frozen ModelResult, and the CallIDs of the calls the result issued, in
// the result's ToolCalls order (from ToolStepOpened). The text, reasoning and
// tool call arguments live in the frozen body; Materialize resolves them.
// RunOwner is what the chatlog remembers of an active Run: the Turn its
// entries belong to and the schema its identities derive under.
type RunOwner struct {
	TurnID TurnID `json:"turnId,omitempty"`
}

type Assistant struct {
	ID           AssistantID        `json:"id"`
	TurnID       TurnID             `json:"turnId,omitempty"`
	RunID        run.RunID          `json:"runId"`
	StepID       run.StepID         `json:"stepId"`
	FinishReason model.FinishReason `json:"finishReason"`
	ResultDigest es.Digest          `json:"resultDigest"`
	CallIDs      []CallID           `json:"callIds,omitempty"`
	Digest       es.Digest          `json:"digest"`
}

// ToolResult is the structural projection of one call's terminal outcome
// (CHT-ENT-2): success names the frozen output or response by digest;
// error and unknown carry the Run-recorded failure.
type ToolResult struct {
	ID           ToolResultID     `json:"id"`
	TurnID       TurnID           `json:"turnId,omitempty"`
	RunID        run.RunID        `json:"runId"`
	CallID       CallID           `json:"callId"`
	Status       ToolResultStatus `json:"status"`
	Source       ToolResultSource `json:"source,omitempty"`
	OutputDigest es.Digest        `json:"outputDigest,omitempty"`
	Failure      *run.ToolFailure `json:"failure,omitempty"`
	Digest       es.Digest        `json:"digest"`
}

// FailureText renders an error or unknown result's failure the way the
// context builder shows it to the model.
func (r *ToolResult) FailureText() string {
	if r.Failure == nil {
		return string(r.Status)
	}
	if r.Failure.Message == "" {
		return r.Failure.Class
	}
	return r.Failure.Class + ": " + r.Failure.Message
}

type Summary struct {
	ID     SummaryID `json:"id"`
	Parts  Parts     `json:"parts"`
	Digest es.Digest `json:"digest"`
}

// AssistantIDFor is the entry identity of a model step's result: the StepID
// itself, which is unique across Runs (TRN-MAP-2).
func AssistantIDFor(step run.StepID) AssistantID { return AssistantID(step) }

// ToolResultIDFor is the entry identity of a call's terminal result: the
// CallID itself.
func ToolResultIDFor(call run.CallID) ToolResultID { return ToolResultID(call) }

// SupersedingToolResultID is the identity of the result an out-of-band
// verification substitutes for id (CHT-ENT-2).
func SupersedingToolResultID(id ToolResultID) ToolResultID { return id + "/superseded" }

// Digests (CHT-COD-3): the domain is the EventType or the entry domain; `v`
// is not covered.

func DigestInput(id InputID, content jsonstable.Value) (es.Digest, error) {
	return digestDomain(string(TypeInputSubmitted), struct {
		ID      InputID          `json:"id"`
		Content jsonstable.Value `json:"content"`
	}{id, content})
}

func DigestAssistant(a *Assistant) (es.Digest, error) {
	return digestDomain(assistantDomain, struct {
		ID           AssistantID        `json:"id"`
		TurnID       TurnID             `json:"turnId,omitempty"`
		RunID        run.RunID          `json:"runId"`
		StepID       run.StepID         `json:"stepId"`
		FinishReason model.FinishReason `json:"finishReason"`
		ResultDigest es.Digest          `json:"resultDigest"`
		CallIDs      []CallID           `json:"callIds,omitempty"`
	}{a.ID, a.TurnID, a.RunID, a.StepID, a.FinishReason, a.ResultDigest, a.CallIDs})
}

func DigestToolResult(r *ToolResult) (es.Digest, error) {
	return digestDomain(toolResultDomain, struct {
		ID           ToolResultID     `json:"id"`
		TurnID       TurnID           `json:"turnId,omitempty"`
		RunID        run.RunID        `json:"runId"`
		CallID       CallID           `json:"callId"`
		Status       ToolResultStatus `json:"status"`
		Source       ToolResultSource `json:"source,omitempty"`
		OutputDigest es.Digest        `json:"outputDigest,omitempty"`
		Failure      *run.ToolFailure `json:"failure,omitempty"`
	}{r.ID, r.TurnID, r.RunID, r.CallID, r.Status, r.Source, r.OutputDigest, r.Failure})
}

func DigestSummary(s *Summary) (es.Digest, error) {
	return digestDomain(string(TypeSummary), struct {
		ID    SummaryID `json:"id"`
		Parts Parts     `json:"parts"`
	}{s.ID, s.Parts})
}

// EntryDigestPair names one active Context entry (CHT-EVT-3).
type EntryDigestPair struct {
	Kind   EntryKind `json:"kind"`
	ID     string    `json:"id"`
	Digest es.Digest `json:"digest"`
}

// DigestBaseContext covers the ordered active Context sequence a compaction
// replaces. An empty base digests as nil (empty and nil are one wire value).
func DigestBaseContext(pairs []EntryDigestPair) (es.Digest, error) {
	if len(pairs) == 0 {
		pairs = nil
	}
	return digestDomain(string(TypeCompactionCreated), struct {
		Base []EntryDigestPair `json:"base"`
	}{pairs})
}

// DigestCompaction covers every compaction field except Digest itself.
func DigestCompaction(p *CompactionCreatedPayload) (es.Digest, error) {
	retained := p.Retained
	if len(retained) == 0 {
		retained = nil
	}
	return digestDomain(string(TypeCompactionCreated), struct {
		CompactionID      CompactionID      `json:"compactionId"`
		CoveredThrough    session.Position  `json:"coveredThrough"`
		BaseContextDigest es.Digest         `json:"baseContextDigest"`
		SummaryID         SummaryID         `json:"summaryId"`
		SummaryDigest     es.Digest         `json:"summaryDigest"`
		Retained          []EntryDigestPair `json:"retained,omitempty"`
	}{p.CompactionID, p.CoveredThrough, p.BaseContextDigest, p.SummaryID, p.SummaryDigest, retained})
}

func digestDomain(domain string, body any) (es.Digest, error) {
	raw, err := es.EncodeTypedPayload(1, domain, body)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(raw), nil
}

// --- payloads (CHT 5) -----------------------------------------------------------

type InputSubmittedPayload struct {
	InputID              InputID          `json:"inputId"`
	Content              jsonstable.Value `json:"content"`
	SubmittedAtUnixMilli int64            `json:"submittedAtUnixMilli"`
}
type InputDeliveredPayload struct {
	InputID InputID `json:"inputId"`
	TurnID  TurnID  `json:"turnId"`
}
type InputWithdrawnPayload struct {
	InputID InputID `json:"inputId"`
	Reason  string  `json:"reason,omitempty"`
}
type InputRejectedPayload struct {
	InputID InputID `json:"inputId"`
	Reason  string  `json:"reason,omitempty"`
}

// ToolResultSupersededPayload substitutes a verified outcome for a call's
// projected result (CHT-ENT-2): an unknown result the Application confirmed
// out of band becomes success (its output frozen under OutputDigest, written
// through the run frozen.Store) or error. The Run fact is untouched; the
// substitution is a conversation decision.
type ToolResultSupersededPayload struct {
	ToolResultID ToolResultID     `json:"toolResultId"`
	Status       ToolResultStatus `json:"status"`
	OutputDigest es.Digest        `json:"outputDigest,omitempty"`
	Reason       string           `json:"reason,omitempty"`
}
type SummaryPayload struct {
	Summary Summary `json:"summary"`
}

// CompactionCreatedPayload compacts the context (CHT-EVT-3): entries up to
// CoveredThrough — the ledger Position of the last covered entry's event —
// are replaced by the summary plus the Retained subset.
type CompactionCreatedPayload struct {
	CompactionID      CompactionID      `json:"compactionId"`
	CoveredThrough    session.Position  `json:"coveredThrough"`
	BaseContextDigest es.Digest         `json:"baseContextDigest"`
	SummaryID         SummaryID         `json:"summaryId"`
	SummaryDigest     es.Digest         `json:"summaryDigest"`
	Retained          []EntryDigestPair `json:"retained,omitempty"`
	Digest            es.Digest         `json:"digest"`
}

type CompactionInvalidatedPayload struct {
	CompactionID CompactionID `json:"compactionId"`
	Reason       string       `json:"reason,omitempty"`
}

func checkSuperseded(p *ToolResultSupersededPayload) error {
	if p.ToolResultID == "" {
		return errors.New("tool_result_superseded requires toolResultId")
	}
	switch p.Status {
	case ToolSuccess:
		if p.OutputDigest == "" {
			return errors.New("tool_result_superseded with status success requires outputDigest")
		}
	case ToolError:
		if p.OutputDigest != "" {
			return errors.New("tool_result_superseded with status error carries no outputDigest")
		}
	default:
		return fmt.Errorf("tool_result_superseded status must be success or error, got %q", p.Status)
	}
	return nil
}

func checkCompactionCreated(p *CompactionCreatedPayload) error {
	if p.CompactionID == "" || p.SummaryID == "" || p.BaseContextDigest == "" || p.SummaryDigest == "" {
		return errors.New("compaction requires compactionId, summaryId and both digests")
	}
	for _, pair := range p.Retained {
		switch pair.Kind {
		case EntryInput, EntryAssistant, EntryToolResult, EntrySummary:
		default:
			return fmt.Errorf("retained entry has unknown kind %q", pair.Kind)
		}
		if pair.ID == "" || pair.Digest == "" {
			return errors.New("retained entry requires id and digest")
		}
	}
	want, err := DigestCompaction(p)
	if err != nil {
		return err
	}
	if p.Digest != want {
		return errors.New("compaction digest mismatch")
	}
	return nil
}

func checkCompactionInvalidated(p *CompactionInvalidatedPayload) error {
	if p.CompactionID == "" {
		return errors.New("compaction_invalidated requires compactionId")
	}
	return nil
}

func checkSummary(p *SummaryPayload) error {
	if p.Summary.ID == "" {
		return errors.New("summary requires id")
	}
	want, err := DigestSummary(&p.Summary)
	if err != nil {
		return err
	}
	if p.Summary.Digest != want {
		return errors.New("summary digest mismatch")
	}
	return nil
}

// PartsExtractor returns the BindingIDs of ReferenceParts in appearance
// order (CHT-COD-2).
var PartsExtractor extension.BindingExtractor = extension.BindingExtractorFunc(func(val any) ([]artifact.BindingID, error) {
	p, ok := val.(SummaryPayload)
	if !ok {
		return nil, fmt.Errorf("parts extractor: unexpected %T", val)
	}
	var out []artifact.BindingID
	for _, part := range p.Summary.Parts {
		if ref, ok := part.(ReferencePart); ok {
			out = append(out, ref.BindingID)
		}
	}
	return out, nil
})

// supersededExtractor names the frozen output a success supersession
// carries, under the run module's frozen Binding derivation.
var supersededExtractor extension.BindingExtractor = extension.BindingExtractorFunc(func(val any) ([]artifact.BindingID, error) {
	p, ok := val.(ToolResultSupersededPayload)
	if !ok {
		return nil, fmt.Errorf("superseded extractor: unexpected %T", val)
	}
	if p.OutputDigest == "" {
		return nil, nil
	}
	return []artifact.BindingID{runmod.FrozenBindingID(p.OutputDigest)}, nil
})

var partsBinding = extension.BindingReferenceDefinition{
	Extractor:          PartsExtractor,
	Cardinality:        extension.Cardinality{Min: 0},
	RequiredDurability: artifact.EventBound,
}

var supersededBinding = extension.BindingReferenceDefinition{
	Extractor:          supersededExtractor,
	Cardinality:        extension.Cardinality{Min: 0},
	AllowedSchemes:     []artifact.Scheme{artifact.SchemeCAS},
	RequiredDurability: artifact.EventBound,
}

// StreamDomain is the singleton stream domain of the chatlog: one stream per
// Session, of session lineage, so a fork continues its parent's conversation.
const StreamDomain = "chatlog"

// Version is the payload version the chatlog writes every event with
// (EXT-REG-2); older versions keep their codecs beside it.
const Version extension.PayloadVersion = 1

var streamDefinition = extension.StreamDefinition{Domain: StreamDomain, Lineage: session.LineageSession}

// Stream is the chatlog's logical stream.
var Stream = streamDefinition.Ref("")

func def[T any](typ session.EventType, check func(*T) error, bindings ...extension.BindingReferenceDefinition) extension.EventDefinition {
	return extension.EventDefinition{Type: typ, Stream: StreamDomain,
		Codecs:   map[extension.PayloadVersion]extension.PayloadCodec{Version: extension.JSONCodec[T]{Check: check}},
		Bindings: bindings}
}

// consumedRunFacts are the Run facts the chatlog projections fold
// (CHT-SCP-1): run_created for the Run's Turn, the model and tool outcomes
// for the entries, tool_step_opened for the CallIDs a result issued, and
// run_ended to forget the Run's Turn.
var consumedRunFacts = []string{"model_step_completed", "tool_step_opened", "tool_call_completed", "tool_call_answered", "tool_call_failed", "run_ended"}

func runRequirement() extension.ModuleRequirement {
	events := make([]session.EventType, 0, len(consumedRunFacts))
	for _, name := range consumedRunFacts {
		events = append(events, runmod.Type(name))
	}
	return extension.ModuleRequirement{Source: extension.SourceTwilight, Module: runmod.ModuleID, Events: events}
}

// Module is the chatlog ModuleDescriptor (CHT-SCP-1: Requires run facts).
var Module = extension.ModuleDescriptor{
	Source:  extension.SourceTwilight,
	ID:      ModuleID,
	Streams: []extension.StreamDefinition{streamDefinition},
	Requires: []extension.ModuleRequirement{runRequirement(),
		// The Turn a Run serves is the attempt module's fact (ATT-1).
		{Source: extension.SourceTwilight, Module: attempt.ModuleID, Events: []session.EventType{attempt.TypeStarted}}},
	Events: []extension.EventDefinition{
		def[InputSubmittedPayload](TypeInputSubmitted, func(p *InputSubmittedPayload) error {
			if p.InputID == "" || p.Content.IsZero() {
				return errors.New("input_submitted requires inputId and content")
			}
			return nil
		}),
		def[InputDeliveredPayload](TypeInputDelivered, func(p *InputDeliveredPayload) error {
			if p.InputID == "" || p.TurnID == "" {
				return errors.New("input_delivered requires inputId and turnId")
			}
			return nil
		}),
		def[InputWithdrawnPayload](TypeInputWithdrawn, nil),
		def[InputRejectedPayload](TypeInputRejected, nil),
		def[ToolResultSupersededPayload](TypeToolResultSuperseded, checkSuperseded, supersededBinding),
		def[SummaryPayload](TypeSummary, checkSummary, partsBinding),
		def[CompactionCreatedPayload](TypeCompactionCreated, checkCompactionCreated),
		def[CompactionInvalidatedPayload](TypeCompactionInvalidated, checkCompactionInvalidated),
	},
	Projections: []extension.ProjectionDefinition{SurfaceProjection, ContextProjection},
}
