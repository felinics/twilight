package artifacttest

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/artifact"
)

// PutLimit checks the MaxBytes contract of one ContentStore implementation:
// newStore returns a store whose Put is capped at maxBytes. A body at the cap
// is stored, a body one byte past it is ErrInvalid, and the largest cap admits
// a body, so the byte a store reads past its cap must not overflow the limit.
func PutLimit(t *testing.T, newStore func(t *testing.T, maxBytes int64) artifact.ContentStore) {
	cases := []struct {
		name     string
		maxBytes int64
		body     string
		wantCode artifact.ErrorCode
	}{
		{"at the cap", 4, "abcd", ""},
		{"one past the cap", 4, "abcde", artifact.ErrInvalid},
		{"largest cap", math.MaxInt64, "abcde", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newStore(t, tc.maxBytes)
			ref, err := store.Put(context.Background(), artifact.PutRequest{MediaType: "text/plain", Reader: strings.NewReader(tc.body), Durability: artifact.Ephemeral})
			if tc.wantCode != "" {
				if !errors.Is(err, &artifact.Error{Code: tc.wantCode}) {
					t.Fatalf("Put(%d bytes, cap %d) = %v, want %s", len(tc.body), tc.maxBytes, err, tc.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("Put(%d bytes, cap %d) = %v", len(tc.body), tc.maxBytes, err)
			}
			if ref.SizeBytes == nil || *ref.SizeBytes != uint64(len(tc.body)) {
				t.Fatalf("Put(%d bytes, cap %d) size = %v", len(tc.body), tc.maxBytes, ref.SizeBytes)
			}
		})
	}
}
