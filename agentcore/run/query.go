package run

// WaitingCalls returns the ResponseRequests of the current ToolStep's
// Waiting calls, in call order. The application inspects them when Next has
// no executable action; each is answered with SubmitToolResponse.
func WaitingCalls(s MachineState) []ResponseRequest { //nolint:gocritic // hugeParam: read-only query over a detached state value
	ts, ok := s.Current.(ToolStep)
	if !ok {
		return nil
	}
	var out []ResponseRequest
	for i := range ts.Calls {
		c := &ts.Calls[i]
		if c.Status != ToolWaiting || c.Waiting == nil {
			continue
		}
		cloned := CloneResponseRequest(c.Waiting)
		if cloned != nil {
			out = append(out, *cloned)
		}
	}
	return out
}

// ExecutingCalls returns CallIDs still Executing on the current ToolStep.
func ExecutingCalls(s MachineState) []CallID { //nolint:gocritic // hugeParam: read-only query over a detached state value
	ts, ok := s.Current.(ToolStep)
	if !ok {
		return nil
	}
	var out []CallID
	for i := range ts.Calls {
		if ts.Calls[i].Status == ToolExecuting {
			out = append(out, ts.Calls[i].CallID)
		}
	}
	return out
}

// NeedsRecovery reports that an execution is in flight and this process has
// no Start effect for it: a ModelStep is Executing, or a ToolStep has
// Executing calls and no Pending calls.
func NeedsRecovery(s MachineState) bool { //nolint:gocritic // hugeParam: read-only query over a detached state value
	switch cur := s.Current.(type) {
	case ModelStep:
		return cur.Status == ModelExecuting
	case ToolStep:
		pending := false
		executing := false
		for i := range cur.Calls {
			c := &cur.Calls[i]
			switch c.Status {
			case ToolPending:
				pending = true
			case ToolExecuting:
				executing = true
			}
		}
		return executing && !pending
	default:
		return false
	}
}
