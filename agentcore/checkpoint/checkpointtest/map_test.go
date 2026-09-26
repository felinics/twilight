package checkpointtest

import (
	"testing"

	"github.com/felinics/twilight/agentcore/checkpoint"
)

func TestMapConformance(t *testing.T) {
	Run(t, func(*testing.T) checkpoint.Store { return &Map{} })
}
