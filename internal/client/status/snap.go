package status

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/dsh2dsh/zrepl/internal/daemon/snapper"
)

func (self *JobRender) renderSnap(r *snapper.Report) {
	defer self.sectionWithTitle("Snapshotting:")()
	s := &self.Styles
	if r == nil {
		s := &self.Styles
		self.printLn(s.Content.Render(s.NotYet.Render()))
		return
	}

	self.printLn(s.Content.Render(fmt.Sprintf("Type: %s", r.Type)))
	if r.Periodic == nil {
		return
	}

	self.renderSnapHeader(r.Periodic)
	if len(r.Periodic.Progress) > 0 {
		self.newline()
		self.renderSnapper(r.Periodic)
	}
}

func (self *JobRender) renderSnapHeader(r *snapper.PeriodicReport) {
	s := &self.Styles
	self.printLn(s.Content.Render(fmt.Sprintf("Status: %s", r.State)))

	if r.Error != "" {
		self.printLn(s.Content.Render(self.indentMultiline(
			"Error:\n"+r.Error, s.Indent)))
	}

	expected, completed := r.CompletionProgress()
	if !r.IsTerminal() {
		var pct float64
		if expected > 0 {
			pct = float64(completed) / float64(expected)
		}
		self.printLn(s.Content.Render(fmt.Sprintf(
			"Progress: %s %d/%d", self.bar.ViewAs(pct), completed, expected)))
	} else if expected > 0 || completed > 0 {
		self.printLn(s.Content.Render(fmt.Sprintf("Completed: %d/%d",
			completed, expected)))
	}
}

func (self *JobRender) renderSnapper(r *snapper.PeriodicReport) {
	items, maxNameLen := self.filterSnapperFs(r.SortProgress())
	self.renderFilesystemsBar(len(r.Progress), len(items))

	s := &self.Styles
	for i := range items {
		item := &items[i]
		states := self.snapperStates(items)
		times := self.snapperTimes(items)
		self.printLn(s.Content.Render(self.viewSnapperFs(
			item.Fs, maxNameLen, item.Matches, states[i], times[i])))
	}
}

func (self *JobRender) filterSnapperFs(items []*snapper.ReportFilesystem,
) (filtered []filteredSnapperFs, maxNameLen int) {
	if self.filterState == FilterApplied {
		filtered, maxNameLen = filterSnapperFs(self.filterValue, items)
	} else {
		filtered, maxNameLen = snapperFsAsFiltered(items)
	}
	return filtered, maxNameLen
}

func (self *JobRender) snapperStates(items []filteredSnapperFs) []string {
	var maxLen int
	states := make([]string, len(items))
	for i := range items {
		fs := items[i].Fs
		switch fs.State {
		case snapper.SnapPending:
			states[i] = hourglassNotDone + fs.State.String()
		case snapper.SnapDone:
			states[i] = checkMarkDone
		case snapper.SnapError:
			states[i] = crossMark
		default:
			states[i] = fs.State.String()
		}
		maxLen = max(maxLen, lipgloss.Width(states[i]))
	}

	s := &self.Styles
	s.SnapState = s.SnapState.Width(maxLen)
	return states
}

func (self *JobRender) snapperTimes(items []filteredSnapperFs) []string {
	var maxLen int
	times := make([]string, len(items))
	for i := range items {
		fs := items[i].Fs
		switch fs.State {
		case snapper.SnapPending:
			times[i] = ellipsis
		case snapper.SnapStarted:
			times[i] = time.Since(fs.StartAt).Truncate(time.Millisecond).String()
		default:
			times[i] = fs.DoneAt.Sub(fs.StartAt).Truncate(time.Millisecond).String()
		}
		maxLen = max(maxLen, lipgloss.Width(times[i]))
	}

	s := self.Styles
	s.SnapTime = s.SnapTime.Width(maxLen)
	return times
}

func (self *JobRender) viewSnapperFs(fs *snapper.ReportFilesystem,
	maxNameLen int, matches []int, state, d string,
) string {
	s := &self.Styles
	var sb strings.Builder

	sb.WriteString(s.InactiveFsIcon.Render())
	sb.WriteString(self.viewFsName(fs.Path, maxNameLen, s.InactiveFs, matches))
	sb.WriteByte(' ')
	sb.WriteString(s.InactiveFs.Render(self.viewSnapperFsStatus(fs, state, d)))
	return sb.String()
}

func (self *JobRender) viewSnapperFsStatus(fs *snapper.ReportFilesystem,
	state, d string,
) string {
	var sb strings.Builder
	s := &self.Styles
	sb.WriteString(fmt.Sprintf("%s %s",
		s.SnapState.Render(state), s.SnapTime.Render(d)))
	if fs.State != snapper.SnapPending {
		sb.WriteString(" @" + fs.SnapName)
	}
	if fs.HooksHadError {
		sb.WriteByte('\n')
		sb.WriteString(s.FsNext.Render(fs.Hooks))
	}
	return s.InactiveFs.Render(sb.String())
}
