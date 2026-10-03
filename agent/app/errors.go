package app

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/run"
)

var (
	errNoModel = errors.New("app: preset requires a model")
	errNilTool = errors.New("app: preset got a nil tool")
)

type duplicateToolError struct{ ref run.ToolRef }

func (e *duplicateToolError) Error() string { return fmt.Sprintf("app: duplicate tool %q", e.ref) }
