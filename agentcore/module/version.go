package module

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// PayloadVersion is the version of one event type's payload codec
// (SES-VER-1, EXT-REG-2). It belongs to the event type, not to the segment:
// every payload carries the version it was written under as `v`, and the
// registry selects the codec by it. The kernel reads none of this.
//
// A version is on one of two tracks. A prerelease version, written pre.N,
// is where a shape is iterated before anything ships: the shape may change
// in place, the number moves so a payload written by an earlier build is
// told apart and read as Unknown rather than misread, and nothing is kept
// for the shape before, so a type has one prerelease codec at a time. A
// stable version, written N, is the release's commitment: the module keeps
// a codec for every stable version it ever published, and every codec of
// one type decodes to the module's current in-memory value, so consumers
// never see a version. Every prerelease version precedes every stable one.
type PayloadVersion struct {
	// Number is the version within its track; zero is no version.
	Number uint16
	// Prerelease puts the version on the prerelease track.
	Prerelease bool
}

// Pre is the prerelease version pre.n.
func Pre(n uint16) PayloadVersion { return PayloadVersion{Number: n, Prerelease: true} }

// Stable is the stable version n.
func Stable(n uint16) PayloadVersion { return PayloadVersion{Number: n} }

// prefix is how a prerelease version is written.
const prefix = "pre."

// IsZero reports no version.
func (v PayloadVersion) IsZero() bool { return v.Number == 0 }

// Less orders versions: no version before every version, every prerelease
// version before every stable one, and by number within a track.
func (v PayloadVersion) Less(o PayloadVersion) bool {
	if v.IsZero() || o.IsZero() {
		return v.IsZero() && !o.IsZero()
	}
	if v.Prerelease != o.Prerelease {
		return v.Prerelease
	}
	return v.Number < o.Number
}

// String is the wire form: "pre.N" or "N"; "" for no version.
func (v PayloadVersion) String() string {
	if v.IsZero() {
		return ""
	}
	if v.Prerelease {
		return prefix + strconv.FormatUint(uint64(v.Number), 10)
	}
	return strconv.FormatUint(uint64(v.Number), 10)
}

// ParsePayloadVersion reads the wire form. The number is a canonical
// decimal in 1..65535; anything else is an error.
func ParsePayloadVersion(s string) (PayloadVersion, error) {
	v := PayloadVersion{}
	num := s
	if strings.HasPrefix(s, prefix) {
		v.Prerelease = true
		num = s[len(prefix):]
	}
	if num == "" || num[0] == '0' {
		return PayloadVersion{}, fmt.Errorf("payload version %q is not pre.N or N with N in 1..65535", s)
	}
	n, err := strconv.ParseUint(num, 10, 16)
	if err != nil {
		return PayloadVersion{}, fmt.Errorf("payload version %q is not pre.N or N with N in 1..65535", s)
	}
	v.Number = uint16(n)
	return v, nil
}

// MarshalText writes the wire form; a zero version is an error, since no
// payload is written without one.
func (v PayloadVersion) MarshalText() ([]byte, error) {
	if v.IsZero() {
		return nil, errors.New("zero payload version")
	}
	return []byte(v.String()), nil
}

// UnmarshalText reads the wire form.
func (v *PayloadVersion) UnmarshalText(b []byte) error {
	parsed, err := ParsePayloadVersion(string(b))
	if err != nil {
		return err
	}
	*v = parsed
	return nil
}
