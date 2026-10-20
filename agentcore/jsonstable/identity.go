package jsonstable

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"unicode/utf8"
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
	// String tuples have no number or object normalization. Escape their
	// strings directly in JCS form, avoiding a marshal/parse/rebuild cycle.
	if tuple, ok := payload.([]string); ok {
		if preimage, ok := encodeStringTuple(schemaVersion, typ, tuple); ok {
			return preimage, nil
		}
	}
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

// encodeStringTuple accepts only valid UTF-8. Invalid strings retain the
// ordinary JSON marshaler's repair semantics through the caller's fallback.
func encodeStringTuple(schemaVersion uint16, typ string, tuple []string) ([]byte, bool) {
	// Reserve the prefix (at most 29 bytes plus typ), delimiters and escapes
	// together, so even strings containing JSON need only one output buffer.
	size := len(typ) + 32
	for _, value := range tuple {
		if !utf8.ValidString(value) {
			return nil, false
		}
		size += len(value) + 3
		for i := 0; i < len(value); i++ {
			switch c := value[i]; c {
			case '"', '\\', '\b', '\f', '\n', '\r', '\t':
				size++
			default:
				if c < 0x20 {
					size += 5
				}
			}
		}
	}
	out := make([]byte, 0, size)
	out = append(out, 'v')
	out = strconv.AppendUint(out, uint64(schemaVersion), 10)
	out = append(out, ':')
	out = strconv.AppendUint(out, uint64(len(typ)), 10)
	out = append(out, ':')
	out = append(out, typ...)
	out = append(out, ':')
	if tuple == nil {
		return append(out, "null"...), true
	}
	out = append(out, '[')
	for i, value := range tuple {
		if i > 0 {
			out = append(out, ',')
		}
		out = appendCanonicalString(out, value)
	}
	return append(out, ']'), true
}

// appendCanonicalString appends RFC 8785 string escaping for valid UTF-8.
// JCS leaves HTML characters, U+2028/U+2029 and non-control Unicode literal.
func appendCanonicalString(out []byte, value string) []byte {
	const hexDigits = "0123456789abcdef"
	out = append(out, '"')
	start := 0
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c >= 0x20 && c != '"' && c != '\\' {
			continue
		}
		out = append(out, value[start:i]...)
		out = append(out, '\\')
		switch c {
		case '"', '\\':
			out = append(out, c)
		case '\b':
			out = append(out, 'b')
		case '\f':
			out = append(out, 'f')
		case '\n':
			out = append(out, 'n')
		case '\r':
			out = append(out, 'r')
		case '\t':
			out = append(out, 't')
		default:
			out = append(out, 'u', '0', '0', hexDigits[c>>4], hexDigits[c&15])
		}
		start = i + 1
	}
	out = append(out, value[start:]...)
	return append(out, '"')
}
