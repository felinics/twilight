package jsonstable

import (
	"bytes"
	"unicode/utf8"
)

// ExtractMember removes one top-level member from an object, returning its
// value and the remaining object. Non-objects, absent members and invalid
// UTF-8 names return ok=false and zero Values. It preserves canonical bytes
// without decoding or canonicalizing values again. Returned Values are
// immutable and may share the input's backing storage.
func (v Value) ExtractMember(name string) (member, remainder Value, ok bool) {
	raw := v.raw
	if len(raw) < 2 || raw[0] != '{' || !utf8.ValidString(name) {
		return Value{}, Value{}, false
	}
	var keyBuffer [64]byte
	key := appendCanonicalString(keyBuffer[:0], name)
	for start := 1; start < len(raw)-1; {
		keyEnd := canonicalStringEnd(raw, start)
		valueStart := keyEnd + 1 // colon
		end := canonicalMemberEnd(raw, valueStart)
		if bytes.Equal(raw[start:keyEnd], key) {
			cutStart, cutEnd := start, end
			if start > 1 {
				cutStart--
			} else if raw[end] == ',' {
				cutEnd++
			}
			body := make([]byte, 0, len(raw)-(cutEnd-cutStart))
			body = append(body, raw[:cutStart]...)
			body = append(body, raw[cutEnd:]...)
			return Value{raw: raw[valueStart:end]}, Value{raw: body}, true
		}
		start = end + 1 // comma
	}
	return Value{}, Value{}, false
}

// These scanners only see the valid, whitespace-free canonical JSON owned
// by Value. Parsing, duplicate-key checks and UTF-8 validation already ran
// at its construction boundary; escaped structural bytes remain in strings.
func canonicalStringEnd(raw []byte, start int) int {
	for i := start + 1; i < len(raw); i++ {
		switch raw[i] {
		case '\\':
			i++
		case '"':
			return i + 1
		}
	}
	panic("jsonstable: invalid canonical string")
}

func canonicalMemberEnd(raw []byte, start int) int {
	depth := 0
	for i := start; i < len(raw); i++ {
		switch raw[i] {
		case '"':
			i = canonicalStringEnd(raw, i) - 1
		case '{', '[':
			depth++
		case '}', ']':
			if depth == 0 {
				return i
			}
			depth--
		case ',':
			if depth == 0 {
				return i
			}
		}
	}
	panic("jsonstable: invalid canonical object")
}
