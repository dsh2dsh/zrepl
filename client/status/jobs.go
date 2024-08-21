package status

import (
	"bytes"
	"cmp"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/list"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/dsh2dsh/zrepl/daemon"
	"github.com/dsh2dsh/zrepl/daemon/job"
)

const (
	defTitle   = "zrepl"
	rightArrow = "➡"
	runner     = "\U0001F3C3"
	sleeping   = "\U0001F4A4"
)

func DefaultItemStyle() (s ItemStyle) {
	s.Time = lipgloss.NewStyle().MarginLeft(1).Width(10).Align(lipgloss.Right)

	s.Running = lipgloss.NewStyle().SetString(runner)
	s.Sleeping = lipgloss.NewStyle().SetString(sleeping)

	s.WithError = lipgloss.NewStyle().
		Border(lipgloss.NormalBorder(), false, false, false, true).
		BorderForeground(lipgloss.AdaptiveColor{
			Light: "#FF0000", Dark: "#FF0000",
		})
	return
}

type ItemStyle struct {
	Time lipgloss.Style

	Running  lipgloss.Style
	Sleeping lipgloss.Style

	WithError lipgloss.Style
}

// --------------------------------------------------

func NewJobDelegate() *JobDelegate {
	return &JobDelegate{
		DefaultDelegate: list.NewDefaultDelegate(),
		Style:           DefaultItemStyle(),
	}
}

type JobDelegate struct {
	list.DefaultDelegate

	Style ItemStyle

	b      bytes.Buffer
	status *daemon.Status

	maxTitle int
}

func (self *JobDelegate) SetStatus(s *daemon.Status, items []ListItem) {
	self.status = s
	for i := range items {
		self.maxTitle = max(self.maxTitle, len(items[i].Title()))
	}
}

func (self *JobDelegate) Render(w io.Writer, m list.Model, index int,
	item list.Item,
) {
	var afterRender func() string
	if item, job := self.job(item); job != nil {
		item.Desc = self.description(job)
		if !self.ShowDescription {
			afterRender = func() string {
				return self.descrStyle(m, item).Render(item.Desc)
			}
		}
	}

	self.DefaultDelegate.Render(w, m, index, item)
	if afterRender != nil {
		fmt.Fprint(w, afterRender())
	}
}

func (self *JobDelegate) descrStyle(m list.Model, item *ListItem,
) (s lipgloss.Style) {
	// https://github.com/charmbracelet/bubbles/blob/364eac96a86724819b8337adbd33630553ee03e6/list/defaultitem.go#L171
	emptyFilter := m.FilterState() == list.Filtering &&
		m.FilterValue() == ""
	listStyles := &self.DefaultDelegate.Styles

	if emptyFilter {
		s = listStyles.DimmedDesc
	} else {
		s = listStyles.NormalDesc
	}

	s = s.MarginLeft(self.maxTitle - len(item.Title()))
	return
}

func (self *JobDelegate) job(item list.Item) (*ListItem, *job.Status) {
	if item, ok := item.(*ListItem); ok {
		if job, ok := self.status.Jobs[item.Title()]; ok {
			return item, job
		}
	}
	return nil, nil
}

func (self *JobDelegate) description(job *job.Status) string {
	defer self.b.Reset()
	self.renderTime(job)
	return self.b.String()
}

func (self *JobDelegate) renderTime(job *job.Status) {
	s := &self.Style
	var withError lipgloss.Style
	if err := job.Error(); err != "" {
		withError = s.WithError
	}

	if d, ok := job.Running(); ok {
		self.b.WriteString(s.Running.Inherit(withError).Render())
		self.b.WriteString(s.Time.Render(d.Truncate(time.Second).String()))
	} else if t := job.SleepingUntil(); !t.IsZero() {
		self.b.WriteString(s.Sleeping.Inherit(withError).Render())
		self.b.WriteString(s.Time.Render(
			time.Until(t).Truncate(time.Second).String()))
	}
}

// --------------------------------------------------

func NewJobsList() *JobsList {
	jobs := &JobsList{
		Choose: key.NewBinding(key.WithKeys("enter"),
			key.WithHelp("enter", "choose")),
	}
	return jobs.init()
}

type JobsList struct {
	Choose key.Binding
	Style  lipgloss.Style

	status   *daemon.Status
	items    []ListItem
	list     *ListModel
	delegate *JobDelegate

	selected func(name string)
}

func (self *JobsList) init() *JobsList {
	self.delegate = self.newJobDelegate()
	l := NewList([]ListItem{}, self.delegate, 0, 0).
		WithItemFunc(self.selectedCmd)
	l.Choose = self.Choose
	l.Style = self.Style
	l.InitDelegate(&self.delegate.DefaultDelegate)
	self.list = l

	ll := l.List()
	ll.Title = "Connecting..."
	ll.SetStatusBarItemName("job", "jobs")
	return self
}

func (self *JobsList) newJobDelegate() *JobDelegate {
	d := NewJobDelegate()
	d.ShowDescription = false
	d.SetSpacing(0)
	return d
}

func (self *JobsList) WithSelected(fn func(name string)) *JobsList {
	self.selected = fn
	return self
}

func (self *JobsList) Update(msg tea.Msg) tea.Cmd {
	if msg, ok := msg.(tea.WindowSizeMsg); ok {
		w, h := self.Style.GetFrameSize()
		self.List().SetSize(msg.Width-w, msg.Height-h)
	}
	return self.list.Update(msg)
}

func (self *JobsList) List() *list.Model { return self.list.List() }

func (self *JobsList) View() string {
	return self.Style.Render(self.list.View())
}

func (self *JobsList) Loading() tea.Cmd {
	return self.List().StartSpinner()
}

func (self *JobsList) SetItems(status *daemon.Status) []ListItem {
	self.status = status
	self.items = self.makeJobItems(status.Jobs)
	self.delegate.SetStatus(status, self.items)
	self.list.SetItems(self.items)

	l := self.List()
	l.StopSpinner()
	l.SetShowStatusBar(true)

	self.Refresh()
	return self.items
}

func (self *JobsList) makeJobItems(jobs map[string]*job.Status) []ListItem {
	items := make([]ListItem, 0, len(jobs))
	for name, j := range jobs {
		if !j.Internal() && j.JobSpecific != nil {
			items = append(items, ListItem{Caption: name})
		}
	}
	slices.SortFunc(items, func(a ListItem, b ListItem) int {
		return cmp.Compare(a.Title(), b.Title())
	})
	return items
}

func (self *JobsList) selectedCmd(item *ListItem) tea.Cmd {
	if self.selected != nil {
		self.selected(item.Title())
	}
	return nil
}

func (self *JobsList) Select(name string) {
	for i := range self.items {
		item := &self.items[i]
		if item.Caption == name {
			self.List().Select(i)
			self.selectedCmd(item)
			return
		}
	}
}

func (self *JobsList) Refresh() {
	self.updateTitle()
}

func (self *JobsList) updateTitle() {
	l := self.List()
	l.Title = defTitle

	var sb strings.Builder
	runCnt, withErr := self.status.JobCounts()
	if runCnt > 0 {
		sb.WriteString(strconv.Itoa(runCnt) + runner)
	}
	if withErr > 0 {
		sb.WriteString(strconv.Itoa(withErr) + crossMark)
	}

	if sb.Len() > 0 {
		l.Title += " " + rightArrow + " " + sb.String()
	}
}
