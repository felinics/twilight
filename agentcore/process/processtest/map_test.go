package processtest

import (
	"testing"

	"github.com/felinics/twilight/agentcore/process"
)

func TestMapConformance(t *testing.T) {
	Run(t, func(*testing.T) process.Store { return &Map{} })
}
