package jsonstable

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/gowebpki/jcs"
)

// Render the original marshal/JCS path independently of the tuple shortcut.
func legacyTypedPayload(version uint16, typ string, payload any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	canonical, err := jcs.Transform(raw)
	if err != nil {
		return nil, err
	}
	return append([]byte(fmt.Sprintf("v%d:%d:%s:", version, len(typ), typ)), canonical...), nil
}

func checkTupleParity(t *testing.T, version uint16, typ string, payload any) {
	t.Helper()
	got, err := EncodeTypedPayload(version, typ, payload)
	want, legacyErr := legacyTypedPayload(version, typ, payload)
	if (err == nil) != (legacyErr == nil) || !bytes.Equal(got, want) {
		t.Fatalf("payload %q: got %q (%v), want %q (%v)", payload, got, err, want, legacyErr)
	}
}

type customTuple []string

func (customTuple) MarshalJSON() ([]byte, error) { return []byte(`{"custom":true}`), nil }

func TestTypedStringTupleLegacyParity(t *testing.T) {
	control := make([]byte, 32)
	for i := range control {
		control[i] = byte(i)
	}
	cases := []any{
		[]string(nil), []string{}, []string{""}, []string{"a", "b"},
		[]string{string(control), `"\/\u0000\ud800`, "<>&/\x7f\u2028\u2029", "中文😀\ufffd"},
		[]string{string([]byte{0xff, 0xc0, 0x80, 0xed, 0xa0, 0x80}), "a\xfez"},
		[]string{strings.Repeat(`{"line":"a\nb","x":"<>&😀"}`, 4000)},
		[]string{"v1:2:a:", "null", "[]"},
		customTuple{"ignored"}, [2]string{"a", "b"}, &[]string{"pointer"},
		map[string]any{"z": 1.0, "a": []any{nil, "<>&"}},
	}
	for i, payload := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			for _, version := range []uint16{0, 1, 65535} {
				checkTupleParity(t, version, "类型:\x00", payload)
			}
		})
	}
	// Exhaust every byte (including malformed UTF-8) and a deterministic corpus
	// of Unicode scalar strings with escape boundaries in different positions.
	for i := 0; i < 256; i++ {
		checkTupleParity(t, 1, "bytes", []string{string([]byte{byte(i)})})
	}
	rng := rand.New(rand.NewPCG(719, 42))
	for i := 0; i < 500; i++ {
		tuple := make([]string, rng.IntN(7))
		for j := range tuple {
			runes := make([]rune, rng.IntN(80))
			for k := range runes {
				r := rune(rng.IntN(0x110000))
				if r >= 0xd800 && r <= 0xdfff {
					r = '\ufffd'
				}
				runes[k] = r
			}
			tuple[j] = string(runes) + "\x00\n\"\\"
		}
		checkTupleParity(t, uint16(i), "tuple", tuple)
	}
}

func FuzzTypedStringTupleLegacyParity(f *testing.F) {
	f.Add("", "", uint16(1))
	f.Add("<>&\u2028\u2029😀", "\x00\b\f\n\r\t\\\"", uint16(65535))
	f.Add("\xff\xed\xa0\x80", `\ud800`, uint16(0))
	f.Fuzz(func(t *testing.T, a, b string, version uint16) {
		checkTupleParity(t, version, a, []string{a, b})
	})
}

func TestValueStringDetached(t *testing.T) {
	for _, value := range []Value{{}, MustParse("null"), MustParse(`{"s":"<>&😀"}`)} {
		s := value.String()
		if s != string(value.Bytes()) {
			t.Fatalf("String mismatch: %q", s)
		}
		detached := value.Bytes()
		detached[0] = '!'
		if value.String() != s {
			t.Fatal("byte accessor modified string or Value")
		}
		copyValue := value
		if err := copyValue.UnmarshalJSON([]byte(`"replacement"`)); err != nil {
			t.Fatal(err)
		}
		if value.String() != s {
			t.Fatal("replacement modified original string or Value")
		}
	}
}

func BenchmarkTypedStringTuple(b *testing.B) {
	payload := []string{"twilight/turn/machine", "1", "128", strings.Repeat(`{"text":"hello <>&😀","n":1}`, 1000)}
	for _, legacy := range []bool{true, false} {
		b.Run(fmt.Sprint("legacy=", legacy), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var err error
				if legacy {
					_, err = legacyTypedPayload(1, "twilight/projection-checkpoint", payload)
				} else {
					_, err = EncodeTypedPayload(1, "twilight/projection-checkpoint", payload)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
