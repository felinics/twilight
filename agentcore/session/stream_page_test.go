package session

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
)

type streamPageStore struct {
	SegmentStore
	headers map[SegmentID]SegmentHeader
	commits map[SegmentID][]ledger.Commit
	missing SegmentID
	read    int
}

func (s *streamPageStore) Index(_ context.Context, id SegmentID) (CommitIndex, ledger.Head, error) {
	idx := BuildCommitIndex(s.headers[id], s.commits[id])
	head := idx.Through
	if id == s.missing {
		idx = CommitIndex{}
	}
	return idx, head, nil
}

func (s *streamPageStore) ReadSegmentStream(_ context.Context, id SegmentID, stream ledger.Domain, from ledger.CommitSeq, limit uint32) ([]ledger.Commit, bool, error) {
	var out []ledger.Commit
	for _, c := range s.commits[id] {
		if c.Seq < from {
			continue
		}
		for _, b := range c.Batches {
			if b.Domain == stream && len(b.Events) > 0 {
				if AtLimit(len(out), limit) {
					return out, true, nil
				}
				out = append(out, c)
				s.read++
				break
			}
		}
	}
	return out, false, nil
}

func streamPageCommits(first, n int) []ledger.Commit {
	var commits []ledger.Commit
	for i := first; i < first+n; i++ {
		var batches []ledger.EventBatch
		for _, stream := range []string{"chat", "turn"} {
			// Uneven event counts, empty batches and gaps in each stream.
			var events []ledger.Event
			for j := 0; j < (i+len(stream))%4; j++ {
				events = append(events, ledger.Event{Type: ledger.EventType(fmt.Sprintf("%s-%d-%d", stream, i, j))})
			}
			batches = append(batches, ledger.EventBatch{Domain: ledger.Domain{Name: stream}, Events: events})
		}
		commits = append(commits, ledger.Commit{Seq: ledger.CommitSeq(i), CommitID: ledger.CommitID(fmt.Sprint(i)), Batches: batches})
	}
	return commits
}

func TestStreamPagesMatchFullReadAcrossFork(t *testing.T) {
	root := SegmentHeader{ID: "root"}
	child := SegmentHeader{ID: "child", Parent: &CommitRef{Segment: "root", Seq: 4}}
	path := LoadedPath{Segments: []LoadedSpan{
		{Segment: Segment{Header: root}, End: ThroughBound(4)},
		{Segment: Segment{Header: child}, From: 5, End: OpenBound()},
	}}
	s := &streamPageStore{headers: map[SegmentID]SegmentHeader{"root": root, "child": child}, commits: map[SegmentID][]ledger.Commit{
		"root":  streamPageCommits(0, 9), // suffix after the fork is invisible
		"child": streamPageCommits(5, 6),
	}}
	ctx := context.Background()
	for _, missing := range []SegmentID{"", "root", "child"} {
		s.missing = missing
		for _, lineage := range []module.Inheritance{module.Own, module.Inherited} {
			for _, name := range []string{"chat", "turn", "absent"} {
				stream := ledger.Domain{Name: name}
				full, err := path.ReadStream(ctx, s, stream, lineage)
				if err != nil {
					t.Fatal(err)
				}
				for from := ledger.StreamSeq(0); from <= 24; from++ {
					for _, limit := range []uint32{0, 1, 2, 5, 25} {
						want, wantMore := StreamEvents(full, stream, from, limit)
						got, more, err := path.readStreamEvents(ctx, s, stream, lineage, from, limit)
						if err != nil || !reflect.DeepEqual(got, want) || more != wantMore {
							t.Fatalf("missing=%s lineage=%s stream=%s from=%d limit=%d: got=%v more=%v err=%v; want=%v more=%v", missing, lineage, name, from, limit, got, more, err, want, wantMore)
						}
					}
				}
			}
		}
	}
}

func TestStreamPageReadsOnlySelectedCommits(t *testing.T) {
	header := SegmentHeader{ID: "root"}
	path := LoadedPath{Segments: []LoadedSpan{{Segment: Segment{Header: header}, End: OpenBound()}}}
	s := &streamPageStore{headers: map[SegmentID]SegmentHeader{"root": header}, commits: map[SegmentID][]ledger.Commit{"root": streamPageCommits(0, 1024)}}
	stream := ledger.Domain{Name: "turn"}
	full := s.commits["root"]
	want, wantMore := StreamEvents(full, stream, 1000, 2)
	got, more, err := path.readStreamEvents(context.Background(), s, stream, module.Own, 1000, 2)
	if err != nil || !reflect.DeepEqual(got, want) || more != wantMore || s.read > 2 {
		t.Fatalf("page=%v more=%v decoded commits=%d err=%v", got, more, s.read, err)
	}
}
