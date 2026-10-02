package effect

// Ports is the effect layer as a composition root declares it: the
// ExecutionPort every drive dispatches through, and the optional
// capabilities the same executor offers. Each optional field is nil when
// the executor lacks it; nothing here is discovered by type assertion.
type Ports struct {
	Execution   ExecutionPort
	Settlements SettlementPort
	Recover     Recoverer
	Progress    ProgressPort
	Ack         Acknowledger
}

// PortsOf fills Ports from one value: Execution is p, each optional field
// is p when p implements it. It is the one place a type assertion is made.
func PortsOf(p ExecutionPort) Ports {
	out := Ports{Execution: p}
	if s, ok := p.(SettlementPort); ok {
		out.Settlements = s
	}
	if r, ok := p.(Recoverer); ok {
		out.Recover = r
	}
	if pr, ok := p.(ProgressPort); ok {
		out.Progress = pr
	}
	if a, ok := p.(Acknowledger); ok {
		out.Ack = a
	}
	return out
}
