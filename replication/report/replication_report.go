package report

import (
	"cmp"
	"encoding/json"
	"slices"
	"time"
)

type Report struct {
	StartAt, FinishAt                      time.Time
	WaitReconnectSince, WaitReconnectUntil time.Time
	WaitReconnectError                     *TimedError
	Attempts                               []*AttemptReport
}

var _, _ = json.Marshal(&Report{})

type TimedError struct {
	Err  string
	Time time.Time
}

func NewTimedError(err string, t time.Time) *TimedError {
	if err == "" {
		panic("error must be empty")
	}
	if t.IsZero() {
		panic("t must be non-zero")
	}
	return &TimedError{err, t}
}

func (s *TimedError) Error() string {
	return s.Err
}

var _, _ = json.Marshal(&TimedError{})

type AttemptReport struct {
	State             AttemptState
	StartAt, FinishAt time.Time
	PlanError         *TimedError
	Filesystems       []*FilesystemReport
}

type AttemptState string

const (
	AttemptPlanning      AttemptState = "planning"
	AttemptPlanningError AttemptState = "planning-error"
	AttemptFanOutFSs     AttemptState = "fan-out-filesystems"
	AttemptFanOutError   AttemptState = "filesystem-error"
	AttemptDone          AttemptState = "done"
)

type FilesystemState string

const (
	FilesystemPlanning        FilesystemState = "planning"
	FilesystemPlanningErrored FilesystemState = "planning-error"
	FilesystemStepping        FilesystemState = "stepping"
	FilesystemSteppingErrored FilesystemState = "step-error"
	FilesystemDone            FilesystemState = "done"
)

type FsBlockedOn string

const (
	FsBlockedOnNothing           FsBlockedOn = "nothing"
	FsBlockedOnPlanningStepQueue FsBlockedOn = "plan-queue"
	FsBlockedOnParentInitialRepl FsBlockedOn = "parent-initial-repl"
	FsBlockedOnReplStepQueue     FsBlockedOn = "repl-queue"
)

type FilesystemReport struct {
	Info *FilesystemInfo

	State FilesystemState

	// Always valid.
	BlockedOn FsBlockedOn

	// Valid in State = FilesystemPlanningErrored
	PlanError *TimedError
	// Valid in State = FilesystemSteppingErrored
	StepError *TimedError

	// Valid in State = FilesystemStepping
	CurrentStep int
	Steps       []*StepReport
}

type FilesystemInfo struct {
	Name string
}

type StepReport struct {
	Info *StepInfo
}

type StepInfo struct {
	From, To        string
	Resumed         bool
	BytesExpected   uint64
	BytesReplicated uint64
}

func (self *AttemptReport) BytesSum() (expected, replicated uint64,
	invalidSizeEstimates bool,
) {
	for _, fs := range self.Filesystems {
		e, r, fsContainsInvalidEstimate := fs.BytesSum()
		invalidSizeEstimates = invalidSizeEstimates || fsContainsInvalidEstimate
		expected += e
		replicated += r
	}
	return
}

func (self *AttemptReport) FilesystemsProgress() (expected, replicated int) {
	expected = len(self.Filesystems)
	for _, fs := range self.Filesystems {
		if fs.State == FilesystemDone {
			replicated++
		}
	}
	return
}

func (self *AttemptReport) FilesystemsByState() map[FilesystemState][]*FilesystemReport {
	r := make(map[FilesystemState][]*FilesystemReport, 4)
	for _, fs := range self.Filesystems {
		l := r[fs.State]
		r[fs.State] = append(l, fs)
	}
	return r
}

func (self *AttemptReport) SortFilesystems() []*FilesystemReport {
	slices.SortFunc(self.Filesystems, func(a, b *FilesystemReport) int {
		if a.Running() && !b.Running() {
			return -1
		} else if !a.Running() && b.Running() {
			return 1
		}
		return cmp.Compare(a.Info.Name, b.Info.Name)
	})
	return self.Filesystems
}

func (self *FilesystemReport) BytesSum() (expected, replicated uint64,
	containsInvalidSizeEstimates bool,
) {
	for _, step := range self.Steps {
		expected += step.Info.BytesExpected
		replicated += step.Info.BytesReplicated
		containsInvalidSizeEstimates = containsInvalidSizeEstimates ||
			step.Info.BytesExpected == 0
	}
	return
}

func (self *FilesystemReport) Error() *TimedError {
	switch self.State {
	case FilesystemPlanningErrored:
		return self.PlanError
	case FilesystemSteppingErrored:
		return self.StepError
	}
	return nil
}

// may return nil
func (self *FilesystemReport) NextStep() *StepReport {
	switch self.State {
	case FilesystemDone:
		return nil
	case FilesystemPlanningErrored:
		return nil
	case FilesystemSteppingErrored:
		return nil
	case FilesystemPlanning:
		return nil
	case FilesystemStepping:
		// invariant is that this is always correct
		// TODO what about 0-length Steps but short intermediary state?
		return self.Steps[self.CurrentStep]
	}
	panic("unreachable")
}

func (self *FilesystemReport) Running() bool {
	return self.BlockedOn == FsBlockedOnNothing &&
		(self.State == FilesystemPlanning || self.State == FilesystemStepping)
}

func (self *FilesystemReport) Step() *StepReport {
	if self.CurrentStep < len(self.Steps) {
		return self.Steps[self.CurrentStep]
	}
	return nil
}

func (f *StepReport) IsIncremental() bool {
	return f.Info.From != ""
}

// Returns, for the latest replication attempt,
// 0  if there have not been any replication attempts,
// -1 if the replication failed while enumerating file systems
// N  if N filesystems could not not be replicated successfully
func (r *Report) GetFailedFilesystemsCountInLatestAttempt() int {
	if len(r.Attempts) == 0 {
		return 0
	}

	a := r.Attempts[len(r.Attempts)-1]
	switch a.State {
	case AttemptPlanningError:
		return -1
	case AttemptFanOutError:
		var count int
		for _, f := range a.Filesystems {
			if f.Error() != nil {
				count++
			}
		}
		return count
	default:
		return 0
	}
}

func (r *Report) Error() string {
	if r.WaitReconnectError != nil {
		return r.WaitReconnectError.Error()
	}

	if len(r.Attempts) == 0 {
		return ""
	}

	att := r.Attempts[len(r.Attempts)-1]
	if att.State == AttemptPlanningError {
		return att.PlanError.Error()
	}

	for _, fs := range att.Filesystems {
		if err := fs.Error(); err != nil {
			return err.Error()
		}
	}
	return ""
}

func (r *Report) Running() (time.Duration, bool) {
	if len(r.Attempts) > 0 {
		att := r.Attempts[len(r.Attempts)-1]
		if att.FinishAt.IsZero() {
			return time.Since(att.StartAt), true
		}
	}
	return 0, false
}

// Returns true in case the AttemptState is a terminal
// state(AttemptPlanningError, AttemptFanOutError, AttemptDone)
func (a AttemptState) IsTerminal() bool {
	switch a {
	case AttemptPlanningError, AttemptFanOutError, AttemptDone:
		return true
	default:
		return false
	}
}
