package status

import (
	"fmt"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/dsh2dsh/zrepl/daemon"
	"github.com/dsh2dsh/zrepl/daemon/job"
)

type tuiState uint

const (
	stateListJobs tuiState = iota
	stateSelected
)

func NewStatusTUI(client *Client) *StatusTUI {
	ui := &StatusTUI{client: client}
	return ui.init()
}

type StatusTUI struct {
	client *Client
	status daemon.Status
	err    error
	state  tuiState

	initialJob string

	updateEvery   time.Duration
	width, height int

	loaded   bool
	jobs     *JobsList
	selected *JobStatus
	jobName  string
}

func (self *StatusTUI) init() *StatusTUI {
	self.jobs = NewJobsList().WithSelected(self.selectJob)
	self.selected = NewJobStatus(self.client, NewJobRender()).
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
	return tea.Sequence(self.jobs.Loading(), func() tea.Msg {
		return self.load()
	})
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
	} else if !self.loaded && self.initialJob != "" {
		if _, ok := s.Jobs[self.initialJob]; !ok {
			return fmt.Errorf("specified job %q doesn't exists", self.initialJob)
		}
	}
	return s
}

func (self *StatusTUI) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
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

	if cmd, ok := self.updateState(msg); ok {
		return self, cmd
	}
	return self, self.jobs.Update(msg)
}

func (self *StatusTUI) updateState(msg tea.Msg) (tea.Cmd, bool) {
	if self.state == stateSelected {
		return self.selected.Update(msg), true
	}
	return nil, false
}

func (self *StatusTUI) handleKeys(msg tea.KeyMsg) tea.Cmd {
	if cmd, ok := self.updateState(msg); ok {
		return cmd
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
		self.jobs.SetItems(&self.status)
		if self.initialJob != "" {
			self.jobs.Select(self.initialJob)
			self.initialJob = ""
		}
	} else if self.state == stateSelected {
		self.selected.SetJob(self.jobName, self.job())
	}
	return self.refreshCmd()
}

func (self *StatusTUI) job() *job.Status {
	return self.status.Jobs[self.jobName]
}

func (self *StatusTUI) View() string {
	if self.state == stateSelected {
		return self.selected.View()
	}
	return self.jobs.View()
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
