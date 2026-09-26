// Package es provides the domain-neutral identity primitives every Twilight
// protocol layer shares: canonical JSON bytes, a versioned type-discriminated
// digest preimage, and the Digest value itself.
//
// It deliberately does not know Run, Session, Queue, command, fact, or
// Runtime semantics, and it owns no storage, record envelope, folding or
// validation: each domain defines its own event ontology, codec and fold on
// top of these primitives. The per-Run record envelope this package once
// carried was superseded by the flat Session log
// (docs/design/agent-session.md).
package es

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/felinics/twilight/agentcore/jsonstable"
)

// Digest is a SHA-256 digest over canonical protocol bytes.
type Digest string

// CausationID is an opaque cross-domain lineage identifier. Its namespace and
// meaning are owned by the domain or application, never by this package.
type CausationID string

// Canonicalize validates and canonicalizes external JSON protocol bytes.
func Canonicalize(raw []byte) ([]byte, error) {
	return jsonstable.Canonicalize(raw)
}

// MarshalCanonical encodes a Go protocol value into canonical JSON bytes.
func MarshalCanonical(v any) ([]byte, error) {
	return jsonstable.MarshalCanonical(v)
}

// DigestBytes computes the stable SHA-256 identity of canonical bytes. The
// caller is responsible for canonicalizing structured input first.
func DigestBytes(data []byte) Digest {
	sum := sha256.Sum256(data)
	return Digest("sha256:" + hex.EncodeToString(sum[:]))
}

// DigestCanonical canonicalizes v and computes its digest.
func DigestCanonical(v any) (Digest, error) {
	body, err := MarshalCanonical(v)
	if err != nil {
		return "", err
	}
	return DigestBytes(body), nil
}

// EncodeTypedPayload renders the common stable digest input for a versioned,
// type-discriminated domain payload. It is intentionally agnostic about which
// schema versions and type names a domain supports.
func EncodeTypedPayload(schemaVersion uint16, typ string, payload any) ([]byte, error) {
	canonical, err := MarshalCanonical(payload)
	if err != nil {
		return nil, err
	}
	prefix := fmt.Sprintf("v%d:%d:%s:", schemaVersion, len(typ), typ)
	return append([]byte(prefix), canonical...), nil
}

// DecodeStrict canonicalizes raw and decodes it into dst as one JSON value:
// unknown fields and trailing data are rejected. Protocol codecs use it so a
// stored or received body is accepted only in the shape they can re-encode.
func DecodeStrict(raw []byte, dst any) error {
	canonical, err := Canonicalize(raw)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(canonical))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return errors.New("es: trailing data after JSON value")
		}
		return err
	}
	return nil
}
