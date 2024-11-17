package pruner

import "time"

type Report struct {
	State              string
	StartedAt          time.Time
	Error              string
	Pending, Completed []FSReport
}

type FSReport struct {
	Filesystem                string
	SnapshotList, DestroyList []SnapshotReport
	SkipReason                FSSkipReason
	LastError                 string
}

type SnapshotReport struct {
	Name       string
	Replicated bool
	Date       time.Time
}

func (self *Report) StateString() (State, error) {
	return StateString(self.State)
}

func (self *Report) IsTerminal() bool {
	switch self.State {
	case "Plan", "Exec":
		return false
	}
	return true
}

func (self *Report) Running() (d time.Duration, ok bool) {
	if !self.StartedAt.IsZero() {
		d = time.Since(self.StartedAt)
	}
	return d, !self.IsTerminal()
}

func (self *Report) Progress() (expected, completed uint64) {
	for i := range self.Pending {
		fs := &self.Pending[i]
		expected += uint64(len(fs.DestroyList))
	}
	for i := range self.Completed {
		fs := &self.Completed[i]
		expected += uint64(len(fs.DestroyList))
		completed += uint64(len(fs.DestroyList))
	}
	return
}
