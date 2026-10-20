package canonicaljson

import "bytes"

// isCanonical recognizes a conservative subset of RFC 8785 without allocating
// a decoded tree. Keys are strictly increasing ASCII (so byte and UTF-16 order
// coincide), strings use only canonical escapes, and numbers are exact safe
// integers. All other forms, including Unicode keys and floating point, are
// handled by the existing JCS implementation. A false result is not an error.
// Transform checks UTF-8 before calling this scanner.
func isCanonical(raw []byte) bool {
	s := canonicalScanner{raw: raw}
	return s.value(0) && s.pos == len(raw)
}

type canonicalScanner struct {
	raw []byte
	pos int
}

func (s *canonicalScanner) take(c byte) bool {
	if s.pos < len(s.raw) && s.raw[s.pos] == c {
		s.pos++
		return true
	}
	return false
}

func (s *canonicalScanner) value(depth int) bool {
	// Keep stack use bounded on hostile input. Deeper documents use JCS.
	if depth >= 128 || s.pos == len(s.raw) {
		return false
	}
	switch s.raw[s.pos] {
	case '{':
		s.pos++
		if s.take('}') {
			return true
		}
		var previous []byte
		for {
			key, ok := s.string(true)
			if !ok || (previous != nil && bytes.Compare(previous, key) >= 0) || !s.take(':') {
				return false
			}
			previous = key
			if !s.value(depth + 1) {
				return false
			}
			if s.take('}') {
				return true
			}
			if !s.take(',') {
				return false
			}
		}
	case '[':
		s.pos++
		if s.take(']') {
			return true
		}
		for {
			if !s.value(depth + 1) {
				return false
			}
			if s.take(']') {
				return true
			}
			if !s.take(',') {
				return false
			}
		}
	case '"':
		_, ok := s.string(false)
		return ok
	case 't':
		return s.literal("true")
	case 'f':
		return s.literal("false")
	case 'n':
		return s.literal("null")
	default:
		return s.integer()
	}
}

func (s *canonicalScanner) literal(want string) bool {
	if len(s.raw)-s.pos < len(want) || string(s.raw[s.pos:s.pos+len(want)]) != want {
		return false
	}
	s.pos += len(want)
	return true
}

func (s *canonicalScanner) integer() bool {
	negative := s.take('-')
	if s.pos == len(s.raw) {
		return false
	}
	if s.take('0') {
		return !negative
	}
	start := s.pos
	var value uint64
	for s.pos < len(s.raw) {
		c := s.raw[s.pos]
		if c < '0' || c > '9' {
			break
		}
		value = value*10 + uint64(c-'0')
		// At most 16 digits fit. Check each digit to avoid integer overflow.
		if value > 9007199254740991 {
			return false
		}
		s.pos++
	}
	return s.pos > start
}

func (s *canonicalScanner) string(key bool) ([]byte, bool) {
	if !s.take('"') {
		return nil, false
	}
	start := s.pos
	for s.pos < len(s.raw) {
		c := s.raw[s.pos]
		s.pos++
		if c == '"' {
			return s.raw[start : s.pos-1], true
		}
		if c < 0x20 || (key && (c >= 0x80 || c == '\\')) {
			return nil, false
		}
		if c != '\\' {
			continue
		}
		if s.pos == len(s.raw) {
			return nil, false
		}
		escape := s.raw[s.pos]
		s.pos++
		switch escape {
		case '"', '\\', 'b', 'f', 'n', 'r', 't':
		case 'u':
			// JCS spells the remaining ASCII controls as lowercase \u00hh.
			if len(s.raw)-s.pos < 4 || s.raw[s.pos] != '0' || s.raw[s.pos+1] != '0' {
				return nil, false
			}
			hi, lo := s.raw[s.pos+2], s.raw[s.pos+3]
			if hi != '0' && hi != '1' {
				return nil, false
			}
			if (lo < '0' || lo > '9') && (lo < 'a' || lo > 'f') {
				return nil, false
			}
			if hi == '0' && (lo == '8' || lo == '9' || lo == 'a' || lo == 'c' || lo == 'd') {
				return nil, false
			}
			s.pos += 4
		default:
			return nil, false
		}
	}
	return nil, false
}
