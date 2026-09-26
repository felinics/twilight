package sdk

import (
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/gowebpki/jcs"
)

// ErrInvalidJSON reports bytes that are not one JSON document, or a document
// with a repeated object member or an escaped lone surrogate.
var ErrInvalidJSON = errors.New("twilightai: not a canonical-encodable JSON document")

// CanonicalJSON re-encodes one JSON document in its RFC 8785 (JCS) form, so
// that equal documents are equal bytes in every language that implements the
// RFC: object members sorted by UTF-16 code units, no insignificant
// whitespace, minimal string escaping, and numbers in their IEEE-754
// binary64 form (2.0 becomes 2, 1e2 becomes 100). Numbers therefore have
// binary64 semantics: an identifier or quantity that must stay exact belongs
// in a JSON string, not a number.
//
// Input that is not valid UTF-8, not exactly one JSON value, an object with
// a repeated member, or a string with an escaped lone surrogate is
// ErrInvalidJSON. The last two have no single meaning, and the JCS parser
// would otherwise resolve them (the last member wins, the surrogate becomes
// U+FFFD) and let two different inputs canonicalize to the same bytes.
func CanonicalJSON(raw []byte) (json.RawMessage, error) {
	if !utf8.Valid(raw) {
		return nil, fmt.Errorf("%w: invalid UTF-8", ErrInvalidJSON)
	}
	if err := rejectEscapedLoneSurrogates(raw); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidJSON, err)
	}
	canonical, err := jcs.Transform(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidJSON, err)
	}
	return json.RawMessage(canonical), nil
}

// canonicalMarshal encodes v with encoding/json and canonicalizes the result,
// so a value that embeds raw JSON (a json.RawMessage field, a map of them)
// encodes canonically too.
func canonicalMarshal(v any) (json.RawMessage, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return CanonicalJSON(raw)
}

// CanonicalProviderOptions returns options with every value re-encoded by
// CanonicalJSON, so a Request that carries them marshals to the same bytes
// for the same options however they were written. Neither the map nor its
// values are modified; a nil or empty map is returned as is.
func CanonicalProviderOptions(options map[string]json.RawMessage) (map[string]json.RawMessage, error) {
	if len(options) == 0 {
		return options, nil
	}
	out := make(map[string]json.RawMessage, len(options))
	for namespace, raw := range options {
		canonical, err := CanonicalJSON(raw)
		if err != nil {
			return nil, fmt.Errorf("provider options for %q: %w", namespace, err)
		}
		out[namespace] = canonical
	}
	return out, nil
}

var errLoneSurrogate = errors.New("escaped lone surrogate")

// rejectEscapedLoneSurrogates refuses strings whose \u escapes do not form
// valid UTF-16, before the JCS parser replaces them with U+FFFD. It checks
// only escaped UTF-16 structure; JSON syntax stays the parser's job, so a
// truncated escape is left for it to report.
func rejectEscapedLoneSurrogates(raw []byte) error {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '"' {
			continue
		}
		i++
		for i < len(raw) {
			switch raw[i] {
			case '"':
				goto nextToken
			case '\\':
				if i+1 >= len(raw) {
					return nil
				}
				if raw[i+1] != 'u' && raw[i+1] != 'U' {
					i += 2
					continue
				}
				if i+6 > len(raw) {
					return nil
				}
				code, ok := parseHex4(raw[i+2 : i+6])
				if !ok {
					return nil
				}
				switch {
				case 0xd800 <= code && code <= 0xdbff:
					if i+12 > len(raw) || raw[i+6] != '\\' || (raw[i+7] != 'u' && raw[i+7] != 'U') {
						return errLoneSurrogate
					}
					low, ok := parseHex4(raw[i+8 : i+12])
					if !ok || low < 0xdc00 || low > 0xdfff {
						return errLoneSurrogate
					}
					i += 12
				case 0xdc00 <= code && code <= 0xdfff:
					return errLoneSurrogate
				default:
					i += 6
				}
			default:
				i++
			}
		}
	nextToken:
	}
	return nil
}

func parseHex4(raw []byte) (rune, bool) {
	if len(raw) != 4 {
		return 0, false
	}
	var n rune
	for _, b := range raw {
		n <<= 4
		switch {
		case '0' <= b && b <= '9':
			n += rune(b - '0')
		case 'a' <= b && b <= 'f':
			n += rune(b-'a') + 10
		case 'A' <= b && b <= 'F':
			n += rune(b-'A') + 10
		default:
			return 0, false
		}
	}
	return n, true
}
