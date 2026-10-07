package app

import (
	"context"
	"testing"
	"time"
)

func TestMergeEventsClosesWhenCommittedStreamEnds(t *testing.T) {
	committed := make(chan Event)
	transient := make(chan Event)
	merged := mergeEvents(context.Background(), committed, transient, func() {})
	close(committed)

	select {
	case _, ok := <-merged:
		if ok {
			t.Fatal("merged stream remained open after committed stream ended")
		}
	case <-time.After(time.Second):
		t.Fatal("merged stream did not close after committed stream ended")
	}
}

func TestMergeEventsKeepsCommittedStreamWhenTransientEnds(t *testing.T) {
	committed := make(chan Event, 1)
	transient := make(chan Event)
	merged := mergeEvents(context.Background(), committed, transient, func() {})
	close(transient)
	committed <- Event{}

	select {
	case _, ok := <-merged:
		if !ok {
			t.Fatal("merged stream closed with its transient stream")
		}
	case <-time.After(time.Second):
		t.Fatal("committed event was not forwarded")
	}
	close(committed)
}
