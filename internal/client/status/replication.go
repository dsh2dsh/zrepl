package status

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/dsh2dsh/zrepl/internal/replication/report"
)

const (
	checkMarkDone   = "✅"
	crossMark       = "❌"
	footprintsEmoji = "\U0001F463"
)

func (self *JobRender) viewReplication(r *report.Report) {
	defer self.sectionWithTitle("Replication:")()
	if r == nil {
		s := &self.Styles
		self.printLn(s.Content.Render(s.NotYet.Render()))
		return
	}

	self.viewReconnect(r)
	if latest := self.viewAttempts(r); latest != nil {
		self.viewLastAttempt(latest)
	}
}

func (self *JobRender) viewReconnect(r *report.Report) {
	s := &self.Styles
	if r.WaitReconnectError != nil {
		self.printLn(s.Content.Render(self.indentMultiline(
			fmt.Sprintf("Connectivity:\n%s", r.WaitReconnectError),
			s.Indent)))
	}

	if r.WaitReconnectSince.IsZero() {
		return
	}

	delta := time.Until(r.WaitReconnectUntil).Round(time.Second)
	if r.WaitReconnectUntil.IsZero() || delta > 0 {
		var until string
		if r.WaitReconnectUntil.IsZero() {
			until = "waiting indefinitely"
		} else {
			until = fmt.Sprintf("hard fail in %s @ %s", delta, r.WaitReconnectUntil)
		}
		self.printLn(s.Content.Render(fmt.Sprintf(
			"Connectivity: reconnecting with exponential backoff (since %s) (%s)",
			r.WaitReconnectSince, until)))
		return
	}

	self.printLn(s.Content.Render(fmt.Sprintf(
		"Connectivity: reconnects reached hard-fail timeout @ %s",
		r.WaitReconnectUntil)))
}

func (self *JobRender) viewAttempts(r *report.Report) *report.AttemptReport {
	s := &self.Styles
	switch len(r.Attempts) {
	case 0:
		self.printLn(s.Content.Render("no attempts made yet"))
		return nil
	case 1:
		self.printLn(s.Content.Render("Attempt #1"))
		return r.Attempts[0]
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(
		"Attempt #%d. Previous attempts failed with the following statuses:",
		len(r.Attempts)))
	for i, a := range r.Attempts[:len(r.Attempts)-1] {
		sb.WriteByte('\n')
		sb.WriteString(s.Indent.Render(fmt.Sprintf(
			"#%d: %s (failed at %s) (ran %s)\n", i+1, a.State, a.FinishAt,
			a.FinishAt.Sub(a.StartAt).Truncate(time.Second))))
	}
	self.printLn(s.Content.Render(sb.String()))
	return r.Attempts[len(r.Attempts)-1]
}

func (self *JobRender) viewLastAttempt(a *report.AttemptReport) {
	self.viewAttemptHeader(a)
	if a.State == report.AttemptPlanning ||
		a.State == report.AttemptPlanningError {
		return
	}

	self.renderAttemptContent(a)
	if len(a.Filesystems) == 0 {
		self.printLn(self.Styles.Content.Render(
			"NOTE: no filesystems were considered for replication!"))
		return
	}

	self.newline()
	items, maxNameLen := self.filterFilesystems(a.SortFilesystems())
	self.renderFilesystemsBar(len(a.Filesystems), len(items))
	s := &self.Styles
	for _, item := range items {
		self.printLn(s.Content.Render(self.viewFilesystem(
			item.Fs, maxNameLen, item.Matches)))
	}
}

func (self *JobRender) viewAttemptHeader(a *report.AttemptReport) {
	s := &self.Styles
	self.printLn(s.Content.Render("Status:", string(a.State)))

	if !a.FinishAt.IsZero() {
		self.printLn(s.Content.Render(fmt.Sprintf(
			"Last Run: %s (lasted %s)", a.FinishAt.Round(time.Second),
			a.FinishAt.Sub(a.StartAt).Round(time.Second))))
	} else {
		self.printLn(s.Content.Render(fmt.Sprintf(
			"Started: %s (lasting %s)", a.StartAt.Round(time.Second),
			time.Since(a.StartAt).Round(time.Second))))
	}

	if a.State == report.AttemptPlanningError {
		self.printLn(s.Content.Render(self.indentMultiline(fmt.Sprintf(
			"Problem:\n%s", a.PlanError), s.Indent)))
	} else if a.State == report.AttemptFanOutError {
		self.printLn(s.Content.Render(
			"Problem: one or more of the filesystems encountered errors"))
	}
}

func (self *JobRender) renderAttemptContent(a *report.AttemptReport) {
	fsNum, fsDone := a.FilesystemsProgress()
	expected, replicated, invalidEstimates := a.BytesSum()
	if a.State.IsTerminal() {
		d := a.FinishAt.Sub(a.StartAt)
		self.renderReplicated(expected, replicated, fsNum, fsDone, d)
	} else {
		d := time.Since(a.StartAt)
		self.renderAttemptProgress(expected, replicated, fsNum, fsDone, d)
	}

	if invalidEstimates {
		self.printLn(self.Styles.Content.Render(
			"NOTE: not all steps could be size-estimated, total estimate is likely imprecise!"))
	}
}

func (self *JobRender) renderReplicated(expected, replicated uint64, fsNum,
	fsDone int, d time.Duration,
) {
	self.printLn(self.Styles.Content.Render(fmt.Sprintf(
		"Replicated: %d/%d, %s / %s, %s",
		fsDone, fsNum,
		humanizeFormat(replicated, true, "%s %sB"),
		humanizeFormat(expected, true, "%s %sB"),
		d.Round(time.Second))))
	if self.speed.Valid() {
		self.speed.Reset()
	}
}

func (self *JobRender) renderAttemptProgress(expected, replicated uint64,
	fsNum, fsDone int, d time.Duration,
) {
	var pct float64
	if expected > 0 {
		pct = float64(replicated) / float64(expected)
	}
	s := &self.Styles
	self.printLn(s.Content.Render("Progress:", self.bar.ViewAs(pct)))

	speed := self.speed.Update(replicated)
	str := fmt.Sprintf("%d/%d, %s / %s @ %s",
		fsDone, fsNum,
		humanizeFormat(replicated, true, "%s %sB"),
		humanizeFormat(expected, true, "%s %sB"),
		humanizeFormat(uint64(speed), true, "%s %sB/s"))
	if replicated < expected && d > 5*time.Second {
		bps := float64(replicated) / d.Seconds()
		bytesLeft := expected - replicated
		eta := time.Duration(float64(bytesLeft)/bps) * time.Second
		str += fmt.Sprintf(" (%s remaining)", humanizeDuration(eta))
	}
	self.printLn(s.Content.Render(s.Indent.Render(str)))
}

func (self *JobRender) filterFilesystems(items []*report.FilesystemReport,
) (filtered []filteredFs, maxNameLen int) {
	if self.filterState == FilterApplied {
		filtered, maxNameLen = filterFilesystems(self.filterValue, items)
	} else {
		filtered, maxNameLen = filesystemsAsFiltered(items)
	}
	return
}

func (self *JobRender) renderFilesystemsBar(totalItems, visibleItems int) {
	if self.filterState != FilterApplied {
		return
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%s “%s” ",
		eyes, strings.TrimSpace(self.filterValue)))
	s := &self.Styles
	switch {
	case visibleItems == 0:
		sb.WriteString(s.StatusEmpty.Render("Nothing matched"))
	case visibleItems == 1:
		sb.WriteString(fmt.Sprintf("%d filesystem", visibleItems))
	default:
		sb.WriteString(fmt.Sprintf("%d filesystems", visibleItems))
	}

	numFiltered := totalItems - visibleItems
	if numFiltered > 0 {
		sb.WriteString(s.DividerDot.Render())
		sb.WriteString(s.StatusBarFilterCount.Render(fmt.Sprintf(
			"%d filtered", numFiltered)))
	}
	self.printLn(s.StatusBar.Render(sb.String()))
}

func (self *JobRender) viewFilesystem(fs *report.FilesystemReport,
	maxNameLen int, matches []int,
) string {
	s := &self.Styles
	item, icon := s.Filesystem(fs.Running())

	var sb strings.Builder
	sb.WriteString(icon.Render())
	sb.WriteString(self.viewFsName(fs.Info.Name, maxNameLen, item, matches))
	sb.WriteByte(' ')
	sb.WriteString(item.Render(self.viewFsStatus(fs)))

	if err := fs.Error(); err != nil {
		sb.WriteByte('\n')
		sb.WriteString(item.Render(s.FsNext.Render(err.Error())))
	} else if nextStep := fs.NextStep(); nextStep != nil {
		sb.WriteByte('\n')
		sb.WriteString(item.Render(s.FsNext.Render(self.viewNextStep(nextStep))))
	}
	return sb.String()
}

func (self *JobRender) viewFsName(name string, maxNameLen int,
	s lipgloss.Style, matches []int,
) string {
	if len(matches) == 0 {
		return s.Width(maxNameLen).Render(name)
	}

	unmatched := s.Inline(true)
	matched := unmatched.Inherit(self.Styles.FilterMatch)
	return s.Width(maxNameLen).Render(lipgloss.StyleRunes(
		name, matches, matched, unmatched))
}

func (self *JobRender) viewFsStatus(fs *report.FilesystemReport) string {
	stepNum := fs.CurrentStep
	curStep := fs.Step()
	if curStep != nil {
		stepNum++
	}
	expected, replicated, invalidEstimates := fs.BytesSum()

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(
		"%s (step %d/%d, %s / %s)",
		filesystemState(fs.State), stepNum, len(fs.Steps),
		humanizeFormat(replicated, true, "%s %sB"),
		humanizeFormat(expected, true, "%s %sB")))

	if invalidEstimates {
		sb.WriteString(" (some steps lack size estimation)")
	} else if curStep != nil && curStep.Info.BytesExpected == 0 {
		sb.WriteString(" (step lacks size estimation)")
	}
	return sb.String()
}

func filesystemState(state report.FilesystemState) string {
	switch state {
	case report.FilesystemPlanningErrored, report.FilesystemSteppingErrored:
		return crossMark + strings.ToUpper(string(state))
	case report.FilesystemStepping:
		return footprintsEmoji + strings.ToUpper(string(state))
	case report.FilesystemDone:
		return checkMarkDone
	}
	return strings.ToUpper(string(state))
}

func (self *JobRender) viewNextStep(nextStep *report.StepReport) string {
	var next string
	if nextStep.IsIncremental() {
		next = fmt.Sprintf("%s %s %s",
			nextStep.Info.From, rightArrow, nextStep.Info.To)
	} else {
		next = "full send " + nextStep.Info.To
	}

	if !nextStep.Info.Resumed {
		return next
	}
	return next + " (resumed)"
}
