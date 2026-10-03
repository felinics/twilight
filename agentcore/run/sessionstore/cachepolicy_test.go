package sessionstore

import (
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"testing"
)

// WriterCachePolicy is the module's answer to "who may cache this projection":
// the Writer may cache every projection except the machine projection, whose
// checkpoints the Runtime owns (RUN-CMT-2), and the interval is the
// deployment's, not a constant of this module.
func TestWriterCachePolicyExcludesTheMachineProjection(t *testing.T) {
	other := module.ProjectionID("twilight/chatlog/surface")
	for _, every := range []ledger.CommitSeq{1, module.DefaultCacheEvery, 4096} {
		policy := WriterCachePolicy(every)
		// The interval has elapsed for any of these, so only the exclusion can
		// decline.
		head := ledger.Head{Next: every + 1}
		if policy(MachineProjectionID, MachineProjection.Version, head, ledger.Head{}, false) {
			t.Errorf("every=%d: the Writer would cache the machine projection", every)
		}
		if policy(MachineProjectionID, MachineProjection.Version, head, ledger.Head{}, true) {
			t.Errorf("every=%d: the Writer would cache the machine projection at Close", every)
		}
		if !policy(other, 1, head, ledger.Head{}, false) {
			t.Errorf("every=%d: another projection is not cached once the interval elapsed", every)
		}
	}
}

// The interval is honoured, including the zero value standing in for the
// default, so a deployment that passes no interval still gets bounded work.
func TestWriterCachePolicyHonoursTheInterval(t *testing.T) {
	other := module.ProjectionID("twilight/chatlog/surface")
	cases := []struct {
		every  ledger.CommitSeq
		head   ledger.CommitSeq
		cached ledger.CommitSeq
		want   bool
	}{
		{every: 8, head: 7, cached: 0, want: false},
		{every: 8, head: 8, cached: 0, want: true},
		{every: 8, head: 12, cached: 5, want: false},
		{every: 8, head: 13, cached: 5, want: true},
		{every: 0, head: module.DefaultCacheEvery - 1, cached: 0, want: false},
		{every: 0, head: module.DefaultCacheEvery, cached: 0, want: true},
	}
	for _, tc := range cases {
		got := WriterCachePolicy(tc.every)(other, 1, ledger.Head{Next: tc.head}, ledger.Head{Next: tc.cached}, false)
		if got != tc.want {
			t.Errorf("every=%d head=%d cached=%d: got %v, want %v", tc.every, tc.head, tc.cached, got, tc.want)
		}
	}
}
