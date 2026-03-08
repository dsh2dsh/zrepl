package status

import (
	"fmt"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/dsh2dsh/zrepl/internal/daemon"
	"github.com/dsh2dsh/zrepl/internal/daemon/job"
)

type StatusTUI struct {
	client *Client
	status daemon.Status
	err    error
	state  tuiState

	darkMode   bool
	initialJob string

	updateEvery   time.Duration
	width, height int
	windowTitle   string

	loaded   bool
	jobs     *JobsList
	selected *JobStatus
	jobName  string
}

var _ tea.Model = (*StatusTUI)(nil)

type tuiState int

const (
	stateListJobs tuiState = iota
	stateSelected
)

func NewStatusTUI(client *Client) *StatusTUI {
	self := &StatusTUI{
		client:      client,
		windowTitle: defTitle,
	}
	return self.init(true)
}

func (self *StatusTUI) init(darkMode bool) *StatusTUI {
	self.darkMode = darkMode
	self.jobs = NewJobsList(darkMode).WithSelected(self.selectJob)
	self.selected = NewJobStatus(darkMode, self.client, NewJobRender(darkMode)).
		WithBackTo(self.backToJobs)
	return self
}

func (self *StatusTUI) WithUpdateEvery(d time.Duration) *StatusTUI {
	self.updateEvery = d
	return self
}

func (self *StatusTUI) WithInitialJob(name string) *StatusTUI {
	self.initialJob = name
	return self
}

func (self *StatusTUI) Init() tea.Cmd {
	return tea.Sequence(tea.RequestBackgroundColor, self.jobs.Loading(),
		self.load)
}

func (self *StatusTUI) refreshCmd() tea.Cmd {
	return tea.Tick(self.updateEvery, func(t time.Time) tea.Msg {
		return self.load()
	})
}

func (self *StatusTUI) load() tea.Msg {
	s, err := self.client.Status()
	if err != nil {
		return err
	}

	if !self.loaded && self.initialJob != "" {
		if _, ok := s.Jobs[self.initialJob]; !ok {
			return fmt.Errorf("specified job %q doesn't exists", self.initialJob)
		}
	}
	return s
}

func (self *StatusTUI) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.BackgroundColorMsg:
		return self, self.SwitchDark(msg.IsDark())
	case tea.KeyMsg:
		return self, self.handleKeys(msg)
	case tea.WindowSizeMsg:
		return self, self.handleWindowSize(msg)
	case error:
		self.err = msg
		return self, tea.Quit
	case daemon.Status:
		return self, self.handleStatus(msg)
	}
	return self, self.updateState(msg)
}

func (self *StatusTUI) handleKeys(msg tea.KeyMsg) tea.Cmd {
	return self.updateState(msg)
}

func (self *StatusTUI) updateState(msg tea.Msg) tea.Cmd {
	if self.state == stateSelected {
		return self.selected.Update(msg)
	}
	return self.jobs.Update(msg)
}

func (self *StatusTUI) handleWindowSize(msg tea.WindowSizeMsg) tea.Cmd {
	self.width, self.height = msg.Width, msg.Height
	return tea.Sequence(self.jobs.Update(msg), self.selected.Update(msg))
}

func (self *StatusTUI) handleStatus(s daemon.Status) tea.Cmd {
	self.status = s

	if !self.loaded {
		self.loaded = true
		self.windowTitle = self.jobs.SetItems(&self.status)
		if self.initialJob != "" {
			self.jobs.Select(self.initialJob)
			self.initialJob = ""
		}
		return self.refreshCmd()
	}

	if self.state == stateSelected {
		self.selected.SetJob(self.jobName, self.job())
	}

	self.windowTitle = self.jobs.RefreshTitle()
	return self.refreshCmd()
}

func (self *StatusTUI) job() *job.Status {
	return self.status.Jobs[self.jobName]
}

func (self *StatusTUI) View() tea.View {
	var s string
	switch self.state {
	case stateListJobs:
		s = self.jobs.View()
	case stateSelected:
		s = self.selected.View()
	}

	v := tea.NewView(s)
	v.AltScreen = true
	v.WindowTitle = self.windowTitle
	return v
}

func (self *StatusTUI) Err() error { return self.err }

func (self *StatusTUI) selectJob(name string) {
	self.jobName = name
	self.selected.SetJob(name, self.job())
	self.state = stateSelected
}

func (self *StatusTUI) backToJobs() {
	self.jobName = ""
	self.selected.Reset()
	self.state = stateListJobs
}

func (self *StatusTUI) SwitchDark(darkMode bool) tea.Cmd {
	if self.darkMode == darkMode {
		return nil
	}

	self.jobs.SwitchDark(darkMode)
	self.selected.SwitchDark(darkMode)
	self.darkMode = darkMode
	return nil
}
