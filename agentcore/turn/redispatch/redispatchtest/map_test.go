package redispatchtest

import (
	"testing"

	"github.com/felinics/twilight/agentcore/turn/redispatch"
)

func TestMapConformance(t *testing.T) {
	Run(t, func(*testing.T) redispatch.Store { return &Map{} })
}
