package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/run"
)

// payloadVersion is the version the typed fact payload bytes carry in
// their prefix; part of the encoded bytes, never a selector.
const payloadVersion uint16 = 1

// CommandEnvelope carries one command with its protocol identity. Commands
// are not persisted: ID is the CommitID of the commit the command produces,
// and replay is told from conflict by the store's command index (RUN-WIR-2,
// EXT-WRT-2). The envelope names no store: which store a command reaches is
// the bound RunStore's.
type CommandEnvelope struct {
	Type    string           `json:"type"`
	RunID   run.RunID        `json:"runId"`
	ID      run.CommandID    `json:"id"`
	Command run.AgentCommand `json:"command"`
}

type commandEnvelopeWire struct {
	Type    string          `json:"type"`
	RunID   run.RunID       `json:"runId"`
	ID      run.CommandID   `json:"id"`
	Command json.RawMessage `json:"command"`
}

type commandEnvelopeMarshal struct {
	Type    string           `json:"type"`
	RunID   run.RunID        `json:"runId"`
	ID      run.CommandID    `json:"id"`
	Command run.AgentCommand `json:"command"`
}

// DecodeCommandEnvelope decodes the command wire shape and restores the
// sealed command variant from Type; malformed or unsupported wire data is
// rejected before it can enter a RunStore.
func DecodeCommandEnvelope(raw []byte) (CommandEnvelope, error) {
	var env CommandEnvelope
	if err := es.DecodeStrict(raw, &env); err != nil {
		return CommandEnvelope{}, err
	}
	return env, nil
}

//nolint:gocritic // hugeParam: value receiver keeps json.Marshaler active for non-pointer CommandEnvelope values.
func (e CommandEnvelope) MarshalJSON() ([]byte, error) {
	if e.Command == nil {
		return nil, errors.New("agent: codec: command envelope has nil command")
	}
	codec := Facts{}
	typ := codec.CommandType(e.Command)
	if typ == "" {
		return nil, fmt.Errorf("agent: codec: unknown command variant %T", e.Command)
	}
	if e.Type != "" && e.Type != typ {
		return nil, fmt.Errorf("agent: codec: command type %q does not match variant %q", e.Type, typ)
	}
	return json.Marshal(commandEnvelopeMarshal{Type: typ, RunID: e.RunID, ID: e.ID, Command: e.Command})
}

func (e *CommandEnvelope) UnmarshalJSON(raw []byte) error {
	var wire commandEnvelopeWire
	if err := es.DecodeStrict(raw, &wire); err != nil {
		return err
	}
	codec := Facts{}
	cmd, err := codec.DecodeCommand(wire.Type, wire.Command)
	if err != nil {
		return err
	}
	if err := requireCanonicalEquivalent(raw, commandEnvelopeMarshal{
		Type: wire.Type, RunID: wire.RunID, ID: wire.ID, Command: cmd,
	}); err != nil {
		return err
	}
	*e = CommandEnvelope{Type: wire.Type, RunID: wire.RunID, ID: wire.ID, Command: cmd}
	return nil
}

func requireCanonicalEquivalent(raw []byte, canonicalShape any) error {
	rawCanonical, err := es.Canonicalize(raw)
	if err != nil {
		return err
	}
	shapeCanonical, err := es.MarshalCanonical(canonicalShape)
	if err != nil {
		return err
	}
	if !bytes.Equal(rawCanonical, shapeCanonical) {
		return errors.New("agent: codec: JSON shape does not match canonical protocol fields")
	}
	return nil
}

// --- v1 wire ------------------------------------------------------------------------

// Facts speaks through variants, its own frozen variant table.
type Facts struct{}

func (Facts) FactType(f run.Fact) string            { return variants.factType(f) }
func (Facts) CommandType(c run.AgentCommand) string { return variants.commandType(c) }
func (Facts) DecodeFact(typ string, raw []byte) (run.Fact, error) {
	return variants.decodeFact(typ, raw)
}

func (Facts) DecodeCommand(typ string, raw []byte) (run.AgentCommand, error) {
	return variants.decodeCommand(typ, raw)
}

func (Facts) EncodeFact(typ string, fact run.Fact) ([]byte, error) {
	if typ == "" || typ != variants.factType(fact) {
		return nil, fmt.Errorf("agent: encode: type %q does not match fact variant", typ)
	}
	return es.EncodeTypedPayload(payloadVersion, typ, fact)
}

func (Facts) Envelope(runID run.RunID, id run.CommandID, cmd run.AgentCommand) (CommandEnvelope, error) {
	typ := variants.commandType(cmd)
	if typ == "" {
		return CommandEnvelope{}, fmt.Errorf("agent: envelope: unknown command variant %T", cmd)
	}
	return CommandEnvelope{Type: typ, RunID: runID, ID: id, Command: cmd}, nil
}

func (Snapshot) Encode(s *run.MachineState) ([]byte, error)  { return encodeMachineState(s) }
func (Snapshot) Decode(raw []byte) (run.MachineState, error) { return decodeMachineState(raw) }
