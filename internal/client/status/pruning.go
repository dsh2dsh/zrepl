package status

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/dsh2dsh/zrepl/internal/daemon/pruner"
)

const hourglassNotDone = "⏳"

func (self *JobRender) renderPruning(title string, p *pruner.Report) {
	defer self.sectionWithTitle(title)()
	if p == nil {
		s := &self.Styles
		self.printLn(s.Content.Render(s.NotYet.Render()))
		return
	}

	state, ok := self.renderPruningState(p)
	if !ok {
		return
	}

	if state.IsTerminal() {
		self.renderPrunedDatasets(p)
	} else {
		self.renderPruningProgress(p)
	}
	self.newline()

	totalItems := sortPrunerFs(p.Pending, p.Completed)
	filteredItems, maxNameLen := self.filterPrunerFs(totalItems)
	self.renderFilesystemsBar(len(totalItems), len(filteredItems))
	for i := range filteredItems {
		self.printLn(self.Styles.Content.Render(
			self.viewPrunerFs(&filteredItems[i], maxNameLen)))
	}
}

func (self *JobRender) renderPruningState(p *pruner.Report,
) (pruner.State, bool) {
	s := &self.Styles
	state, err := p.StateString()
	if err != nil {
		self.printLn(s.Content.Render(fmt.Sprintf(
			"Status: %q (parse error: %q)", p.State, err)))
		return state, false
	}

	status := fmt.Sprintf("Status: %s", state)
	totalItems := len(p.Pending) + len(p.Completed)
	if totalItems == 0 {
		status += " (nothing to do)"
	}
	self.printLn(s.Content.Render(status))

	if p.Error != "" {
		self.printLn(s.Content.Render(self.indentMultiline(
			"Error:\n"+p.Error, s.Indent)))
	}

	if totalItems == 0 || state == pruner.Plan || state == pruner.PlanErr {
		return state, false
	}
	return state, true
}

func (self *JobRender) renderPrunedDatasets(p *pruner.Report) {
	totalItems := len(p.Pending) + len(p.Completed)
	self.printLn(self.Styles.Content.Render(fmt.Sprintf(
		"Pruned: %d/%d", len(p.Completed), totalItems)))
}

func (self *JobRender) renderPruningProgress(p *pruner.Report) {
	expected, completed := p.Progress()
	pct := float64(completed) / float64(expected)
	s := &self.Styles
	self.printLn(s.Content.Render(fmt.Sprintf(
		"Progress: %s %d/%d", self.bar.ViewAs(pct), completed, expected)))
}

func (self *JobRender) filterPrunerFs(items []prunerFs,
) (filtered []prunerFs, maxNameLen int) {
	if self.filterState == FilterApplied {
		filtered, maxNameLen = filterPrunerFs(self.filterValue, items)
	} else {
		filtered, maxNameLen = prunerFsAsFiltered(items)
	}
	return
}

func sortPrunerFs(pending []pruner.FSReport, completed []pruner.FSReport,
) []prunerFs {
	items := make([]prunerFs, 0, len(pending)+len(completed))
	for i := range pending {
		item := &pending[i]
		items = append(items, prunerFs{FSReport: item})
	}

	for i := range completed {
		item := &completed[i]
		items = append(items, prunerFs{FSReport: item, Completed: true})
	}

	slices.SortFunc(items, func(a, b prunerFs) int {
		return cmp.Compare(a.FSReport.Filesystem, b.FSReport.Filesystem)
	})
	return items
}

type prunerFs struct {
	*pruner.FSReport

	Completed bool
	Matches   []int
}

func (self *JobRender) viewPrunerFs(item *prunerFs, maxNameLen int) string {
	s := &self.Styles
	var sb strings.Builder

	sb.WriteString(s.InactiveFsIcon.Render())
	fs := item.FSReport
	sb.WriteString(self.viewFsName(
		fs.Filesystem, maxNameLen, s.InactiveFs, item.Matches))
	sb.WriteByte(' ')
	sb.WriteString(s.InactiveFs.Render(
		self.viewPrunerFsStatus(fs, item.Completed)))

	return sb.String()
}

func (self *JobRender) viewPrunerFsStatus(fs *pruner.FSReport, completed bool,
) string {
	if !fs.SkipReason.NotSkipped() {
		return fmt.Sprintf("skipped: %s", fs.SkipReason)
	} else if fs.LastError != "" {
		s := &self.Styles
		return self.indentMultiline(
			fmt.Sprintf("%sERROR:\n%s", crossMark, fs.LastError),
			s.Indent.PaddingLeft(s.InactiveFsIcon.GetWidth()))
	}

	if completed {
		return fmt.Sprintf("%s (destroy %d of %d snapshots)",
			checkMarkDone, len(fs.DestroyList), len(fs.SnapshotList))
	}

	if len(fs.DestroyList) == 1 {
		return fmt.Sprintf("%sPending %s", hourglassNotDone,
			fs.DestroyList[0].Name)
	}
	return fmt.Sprintf("%sPending (destroy %d of %d snapshots)",
		hourglassNotDone, len(fs.DestroyList), len(fs.SnapshotList))
}
