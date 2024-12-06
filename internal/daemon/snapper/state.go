package snapper

import "strconv"

type State uint

const (
	Stopped State = iota
	SyncUp
	SyncUpErrWait
	Planning
	Snapshotting
	ErrorWait
)

func (self State) sf() state {
	switch self {
	case SyncUp:
		return periodicStateSyncUp
	case SyncUpErrWait:
		return nil
	case Planning:
		return periodicStatePlan
	case Snapshotting:
		return periodicStateSnapshot
	case ErrorWait:
		return nil
	}
	return nil // Stopped
}

type (
	updater func(u func(*Periodic)) State
	state   func(a periodicArgs, u updater) state
)

func (self State) String() string {
	switch self {
	case SyncUp:
		return "SyncUp"
	case SyncUpErrWait:
		return "SyncUpErrWait"
	case Planning:
		return "Planning"
	case Snapshotting:
		return "Snapshotting"
	case ErrorWait:
		return "ErrorWait"
	case Stopped:
		return "Stopped"
	}
	return "State(" + strconv.FormatInt(int64(self), 10) + ")"
}

func (self State) IsTerminal() bool {
	return self != Planning && self != Snapshotting
}

func (self State) Running() bool {
	return self == SyncUp || self == Planning || self == Snapshotting
}
