package jsonstable

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Digest is a SHA-256 digest over canonical protocol bytes.
type Digest string

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
			return errors.New("jsonstable: trailing data after JSON value")
		}
		return err
	}
	return nil
}
