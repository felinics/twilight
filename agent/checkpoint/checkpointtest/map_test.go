package checkpointtest

import (
	"testing"

	"github.com/felinics/twilight/agent/checkpoint"
)

func TestMapConformance(t *testing.T) {
	Run(t, func(*testing.T) checkpoint.Store { return &Map{} })
}
