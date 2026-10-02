package session

import (
	"testing"
)

func TestPathCut(t *testing.T) {
	root := SegmentID("A")
	mid := SegmentID("B")
	p := Path{
		{Segment: root, From: 0, End: ThroughBound(2)},
		{Segment: mid, From: 3, End: OpenBound()},
	}
	got, edge, err := p.Branch(4, "C")
	if err != nil {
		t.Fatal(err)
	}
	if edge != (CommitRef{Segment: mid, Seq: 4}) || len(got) != 3 {
		t.Fatalf("cut last span = %+v edge %+v", got, edge)
	}
	if got[1].End != ThroughBound(4) || got[2] != (Span{Segment: "C", From: 5, End: OpenBound()}) {
		t.Fatalf("cut last span = %+v", got)
	}
	if err := got.Validate("C"); err != nil {
		t.Fatal(err)
	}

	got, edge, err = p.Branch(1, "D")
	if err != nil {
		t.Fatal(err)
	}
	want := Path{
		{Segment: root, From: 0, End: ThroughBound(1)},
		{Segment: "D", From: 2, End: OpenBound()},
	}
	if edge != (CommitRef{Segment: root, Seq: 1}) || len(got) != len(want) {
		t.Fatalf("cut earlier span = %+v edge %+v", got, edge)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("span %d = %+v, want %+v", i, got[i], want[i])
		}
	}

	gapped := Path{
		{Segment: root, From: 0, End: ThroughBound(1)},
		{Segment: mid, From: 4, End: OpenBound()},
	}
	if _, _, err := gapped.Branch(3, "E"); err == nil {
		t.Fatal("seq in a gap was accepted")
	}
	closed := Path{{Segment: root, From: 0, End: ThroughBound(1)}}
	if _, _, err := closed.Branch(4, "E"); err == nil {
		t.Fatal("seq past a closed path was accepted")
	}
	var empty Path
	if _, _, err := empty.Branch(0, "E"); err == nil {
		t.Fatal("empty path was accepted")
	}
}

func TestMaxBound(t *testing.T) {
	if _, ok := MaxBound(nil); ok {
		t.Fatal("empty cover set reported a bound")
	}
	got, ok := MaxBound([]Bound{ThroughBound(1), ThroughBound(4)})
	if !ok || got != ThroughBound(4) {
		t.Fatalf("max = %+v %v", got, ok)
	}
	got, ok = MaxBound([]Bound{ThroughBound(4), OpenBound()})
	if !ok || got != OpenBound() {
		t.Fatalf("open max = %+v %v", got, ok)
	}
}

func TestPathValidate(t *testing.T) {
	ok := Path{{Segment: "A", From: 0, End: OpenBound()}}
	if err := ok.Validate("A"); err != nil {
		t.Fatal(err)
	}
	bad := []Path{
		{{Segment: "A", From: 0, End: ThroughBound(1)}},
		{{Segment: "A", From: 0, End: OpenBound()}},
		{
			{Segment: "A", From: 0, End: OpenBound()},
			{Segment: "B", From: 1, End: OpenBound()},
		},
		{
			{Segment: "A", From: 0, End: ThroughBound(1)},
			{Segment: "A", From: 2, End: OpenBound()},
		},
		{
			{Segment: "A", From: 1, End: OpenBound()},
		},
	}
	tips := []SegmentID{"A", "B", "B", "A", "A"}
	for i, p := range bad {
		if err := p.Validate(tips[i]); err == nil {
			t.Fatalf("path %d validated: %+v", i, p)
		}
	}
}
