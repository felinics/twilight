package inboxtest

import (
	"testing"

	"github.com/felinics/twilight/agentcore/inbox"
)

func TestMapConformance(t *testing.T) {
	Run(t, func(*testing.T) inbox.Store { return &Map{} })
}
