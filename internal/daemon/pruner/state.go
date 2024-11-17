package pruner

//go:generate enumer -type=State
type State int

const (
	Plan State = 1 << iota
	PlanErr
	Exec
	ExecErr
	Done
)

// Returns true in case the State is a terminal state(PlanErr, ExecErr, Done)
func (s State) IsTerminal() bool {
	switch s {
	case PlanErr, ExecErr, Done:
		return true
	default:
		return false
	}
}
