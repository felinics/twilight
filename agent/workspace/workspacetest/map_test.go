package workspacetest

import (
	"testing"

	"github.com/felinics/twilight/agent/workspace"
)

func TestMapConformance(t *testing.T) {
	Run(t, func(*testing.T) workspace.Store { return &Map{} })
}
