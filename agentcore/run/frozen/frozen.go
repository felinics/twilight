package frozen

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/canonical"
	"github.com/felinics/twilight/agentcore/run/model"
)

// Store is the run layer's port to the content-addressed side store for
// the bodies that facts name by digest only (RUN-WIR-4): the
// ModelRequest of a Prepared step, the ModelResult of a completed step, a
// tool output and an external tool response. The digest is the SHA-256 of
// the stored bytes, so a body is one cas entry whose key is its digest; the
// adapter that realizes this port over a cas ContentStore lives in
// agent/session/run. Put is idempotent. The fact that names a body is its
// retention root: the body stays readable as long as the Session ledger
// holds the fact.
type Store interface {
	Put(ctx context.Context, digest run.Digest, val []byte) error
	Get(ctx context.Context, digest run.Digest) ([]byte, bool, error)
}

// Authority is the cas Authority under which frozen bodies are stored.
// It is a string, not an artifact type, because run does not depend on
// artifact; the adapter converts it.
const Authority = "twilight/run/frozen"

// ErrMissing reports that a body named by a fact is not in the Store. For a
// request, recovery cannot resend it and the caller
// decides whether to retry the attempt with a fresh plan; for a result or an
// output, the structural projection stays valid and only materialization
// fails.
var ErrMissing = errors.New("agent: frozen value missing")

// envelopeVersion is the version the frozen-body envelope prefix carries;
// it is part of the stored bytes and of the digests computed from them.
const envelopeVersion uint16 = 1

// Bodies is the frozen-body codec. Each body is stored as the
// versioned envelope its digest is computed from (canonical.Bodies), under the
// envelope type canonical declares for it, so sha256(bytes) == digest.
type Bodies struct{}

func (Bodies) EncodeRequest(req *model.ModelRequest, want run.Digest) ([]byte, error) {
	return encodeFrozen(envelopeVersion, canonical.RequestType, *req, want)
}

func (Bodies) EncodeModelResult(result *model.ModelResult, want run.Digest) ([]byte, error) {
	return encodeFrozen(envelopeVersion, canonical.ModelResultType, *result, want)
}

func (Bodies) EncodeToolOutput(output run.CanonicalJSON, want run.Digest) ([]byte, error) {
	if output.IsZero() {
		return nil, errors.New("agent: frozen tool output: empty output")
	}
	return encodeFrozen(envelopeVersion, canonical.ToolOutputType, canonical.ToolOutputBody{Output: output}, want)
}

func (Bodies) EncodeToolResponse(payload run.CanonicalJSON, want run.Digest) ([]byte, error) {
	return encodeFrozen(envelopeVersion, canonical.ToolResponseType, canonical.ToolResponsePayloadBody{Payload: payload}, want)
}

func (Bodies) DecodeRequest(raw []byte, want run.Digest) (model.ModelRequest, error) {
	return decodeFrozen[model.ModelRequest](raw, envelopeVersion, canonical.RequestType, want)
}

func (Bodies) DecodeModelResult(raw []byte, want run.Digest) (model.ModelResult, error) {
	return decodeFrozen[model.ModelResult](raw, envelopeVersion, canonical.ModelResultType, want)
}

func (Bodies) DecodeToolOutput(raw []byte, want run.Digest) (run.CanonicalJSON, error) {
	body, err := decodeFrozen[canonical.ToolOutputBody](raw, envelopeVersion, canonical.ToolOutputType, want)
	if err != nil {
		return run.CanonicalJSON{}, err
	}
	return body.Output, nil
}

func (Bodies) DecodeToolResponse(raw []byte, want run.Digest) (run.CanonicalJSON, error) {
	body, err := decodeFrozen[canonical.ToolResponsePayloadBody](raw, envelopeVersion, canonical.ToolResponseType, want)
	if err != nil {
		return run.CanonicalJSON{}, err
	}
	return body.Payload, nil
}

// checkEnvelope verifies a stored body carries the envelope prefix this
// package writes; the version in it is part of the bytes, not a selector.
func checkEnvelope(raw []byte) error {
	var version uint16
	if _, err := fmt.Sscanf(string(raw[:min(len(raw), 8)]), "v%d:", &version); err != nil {
		return errors.New("agent: frozen body: not a typed envelope")
	}
	if version != envelopeVersion {
		return fmt.Errorf("agent: frozen body: unknown envelope version %d", version)
	}
	return nil
}

// DecodeRequest restores a request body and checks it still digests to
// the name it was stored under. The version comes from the body itself.
func DecodeRequest(raw []byte, want run.Digest) (model.ModelRequest, error) {
	if err := checkEnvelope(raw); err != nil {
		return model.ModelRequest{}, err
	}
	codec := Bodies{}
	return codec.DecodeRequest(raw, want)
}

// DecodeModelResult restores a model result named by ResultDigest.
func DecodeModelResult(raw []byte, want run.Digest) (model.ModelResult, error) {
	if err := checkEnvelope(raw); err != nil {
		return model.ModelResult{}, err
	}
	codec := Bodies{}
	return codec.DecodeModelResult(raw, want)
}

// DecodeToolOutput restores a tool output named by OutputDigest.
func DecodeToolOutput(raw []byte, want run.Digest) (run.CanonicalJSON, error) {
	if err := checkEnvelope(raw); err != nil {
		return run.CanonicalJSON{}, err
	}
	codec := Bodies{}
	return codec.DecodeToolOutput(raw, want)
}

// DecodeToolResponse restores an external tool response named by
// ResponseDigest.
func DecodeToolResponse(raw []byte, want run.Digest) (run.CanonicalJSON, error) {
	if err := checkEnvelope(raw); err != nil {
		return run.CanonicalJSON{}, err
	}
	codec := Bodies{}
	return codec.DecodeToolResponse(raw, want)
}

func encodeFrozen(version uint16, typ string, body any, want run.Digest) ([]byte, error) { //nolint:unparam // version is the caller schema's; only v1 exists today.
	raw, err := es.EncodeTypedPayload(version, typ, body)
	if err != nil {
		return nil, err
	}
	if got := es.DigestBytes(raw); got != want {
		return nil, fmt.Errorf("agent: frozen %s: body digest %s does not match %s", typ, got, want)
	}
	return raw, nil
}

func decodeFrozen[T any](raw []byte, version uint16, typ string, want run.Digest) (T, error) {
	var zero T
	if got := es.DigestBytes(raw); got != want {
		return zero, fmt.Errorf("agent: frozen %s: stored body digest %s does not match %s", typ, got, want)
	}
	prefix := envelopePrefix(version, typ)
	if !bytes.HasPrefix(raw, prefix) {
		return zero, fmt.Errorf("agent: frozen %s: stored body is not a %s envelope", typ, typ)
	}
	var out T
	if err := es.DecodeStrict(raw[len(prefix):], &out); err != nil {
		return zero, fmt.Errorf("agent: frozen %s: %w", typ, err)
	}
	return out, nil
}

// envelopePrefix is the domain prefix EncodeTypedPayload puts before the
// canonical body (agent/es).
func envelopePrefix(schemaVersion uint16, typ string) []byte {
	return []byte(fmt.Sprintf("v%d:%d:%s:", schemaVersion, len(typ), typ))
}
