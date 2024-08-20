package status

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/help"
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/reflow/truncate"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/dsh2dsh/zrepl/daemon/job"
)

const (
	bullet   = "•"
	ellipsis = "…"
	eyes     = "\U0001F440"
)

type jobState uint

const (
	stateJobView jobState = iota
	stateJobSignal
)

type FilterState uint

const (
	Unfiltered FilterState = iota
	Filtering
	FilterApplied
)

var titler = cases.Title(language.English)

func DefaultJobStyles() (s JobStyles) {
	subduedColor := lipgloss.AdaptiveColor{Light: "#9B9B9B", Dark: "#5C5C5C"}
	verySubduedColor := lipgloss.AdaptiveColor{Light: "#DDDADA", Dark: "#3C3C3C"}

	s.TitleBar = lipgloss.NewStyle().Padding(0, 0, 1, 2)
	s.Title = lipgloss.NewStyle().
		Background(lipgloss.Color("62")).
		Foreground(lipgloss.Color("230")).
		Padding(0, 1)

	s.FilterPrompt = lipgloss.NewStyle().
		Foreground(lipgloss.AdaptiveColor{Light: "#04B575", Dark: "#ECFD65"})

	s.FilterCursor = lipgloss.NewStyle().
		Foreground(lipgloss.AdaptiveColor{Light: "#EE6FF8", Dark: "#EE6FF8"})

	s.StatusBar = lipgloss.NewStyle().
		Foreground(lipgloss.AdaptiveColor{Light: "#A49FA5", Dark: "#777777"})

	s.View = lipgloss.NewStyle()

	s.ArabicPagination = lipgloss.NewStyle().
		Foreground(subduedColor).
		Width(4).
		Align(lipgloss.Right).
		MarginRight(1)

	s.Pagination = lipgloss.NewStyle().Padding(0, 2).MarginTop(1)

	s.Help = lipgloss.NewStyle().Padding(1, 0, 0, 2)

	s.ActivePaginationDot = lipgloss.NewStyle().
		Foreground(lipgloss.AdaptiveColor{Light: "#847A85", Dark: "#979797"}).
		SetString(bullet)

	s.InactivePaginationDot = lipgloss.NewStyle().
		Foreground(verySubduedColor).
		SetString(bullet)

	s.DividerDot = lipgloss.NewStyle().
		Foreground(verySubduedColor).
		SetString(bullet)
	return
}

type JobStyles struct {
	TitleBar     lipgloss.Style
	Title        lipgloss.Style
	FilterPrompt lipgloss.Style
	FilterCursor lipgloss.Style

	StatusBar lipgloss.Style

	View lipgloss.Style

	Pagination lipgloss.Style
	Help       lipgloss.Style

	ActivePaginationDot   lipgloss.Style
	InactivePaginationDot lipgloss.Style
	ArabicPagination      lipgloss.Style

	DividerDot lipgloss.Style
}

// --------------------------------------------------

func DefaultJobKeys() JobKeys {
	return JobKeys{
		GoToStart: key.NewBinding(
			key.WithKeys("home", "g"),
			key.WithHelp("g/home", "go to start"),
		),
		GoToEnd: key.NewBinding(
			key.WithKeys("end", "G"),
			key.WithHelp("G/end", "go to end"),
		),

		Filter: key.NewBinding(
			key.WithKeys("/"),
			key.WithHelp("/", "filter"),
		),
		ClearFilter: key.NewBinding(
			key.WithKeys("esc"),
			key.WithHelp("esc", "clear filter"),
		),

		CancelWhileFiltering: key.NewBinding(
			key.WithKeys("esc"),
			key.WithHelp("esc", "cancel"),
		),
		AcceptWhileFiltering: key.NewBinding(
			key.WithKeys("enter", "tab", "shift+tab", "ctrl+k", "up",
				"ctrl+j", "down"),
			key.WithHelp("enter", "apply filter"),
		),

		Jump: key.NewBinding(
			key.WithKeys("tab"),
			key.WithHelp("tab", "jump")),

		Signal: key.NewBinding(
			key.WithKeys("s"),
			key.WithHelp("s", "signal")),

		Back: key.NewBinding(
			key.WithKeys("esc"),
			key.WithHelp("esc", "back")),

		ShowFullHelp: key.NewBinding(
			key.WithKeys("?"),
			key.WithHelp("?", "more"),
		),
		CloseFullHelp: key.NewBinding(
			key.WithKeys("?"),
			key.WithHelp("?", "close help"),
		),

		Quit: key.NewBinding(
			key.WithKeys("q"),
			key.WithHelp("q", "quit")),
		ForceQuit: key.NewBinding(key.WithKeys("ctrl+c")),
	}
}

type JobKeys struct {
	GoToStart   key.Binding
	GoToEnd     key.Binding
	Filter      key.Binding
	ClearFilter key.Binding

	CancelWhileFiltering key.Binding
	AcceptWhileFiltering key.Binding

	Jump   key.Binding
	Back   key.Binding
	Signal key.Binding

	ShowFullHelp  key.Binding
	CloseFullHelp key.Binding

	Quit      key.Binding
	ForceQuit key.Binding
}

func (self *JobKeys) SetEnabled(v bool) {
	self.GoToStart.SetEnabled(v)
	self.GoToEnd.SetEnabled(v)
	self.Filter.SetEnabled(v)
	self.ClearFilter.SetEnabled(v)

	self.CancelWhileFiltering.SetEnabled(v)
	self.AcceptWhileFiltering.SetEnabled(v)

	self.Jump.SetEnabled(v)
	self.Back.SetEnabled(v)
	self.Signal.SetEnabled(v)

	self.ShowFullHelp.SetEnabled(v)
	self.CloseFullHelp.SetEnabled(v)

	self.Quit.SetEnabled(v)
}

// --------------------------------------------------

func NewJobStatus(client *Client, render *JobRender) *JobStatus {
	styles := DefaultJobStyles()

	filterInput := textinput.New()
	filterInput.Prompt = "Filter: "
	filterInput.PromptStyle = styles.FilterPrompt
	filterInput.Cursor.Style = styles.FilterCursor
	filterInput.CharLimit = 64
	filterInput.Focus()

	j := &JobStatus{
		Styles:      styles,
		Keys:        DefaultJobKeys(),
		Help:        help.New(),
		FilterInput: filterInput,

		StatusMessageLifetime: 2 * time.Second,

		client: client,
		render: render,
	}

	j.updateKeybindings()
	return j
}

type JobStatus struct {
	Styles JobStyles
	Keys   JobKeys

	Help        help.Model
	FilterInput textinput.Model
	filterState FilterState

	StatusMessageLifetime time.Duration

	client    *Client
	job       *job.Status
	name      string
	canSignal string

	render     *JobRender
	backToFunc func()

	viewport viewport.Model

	height        int
	heightChanged bool
	width         int

	ready bool
	state jobState

	signalYesNo *ListModel

	statusMessage string
	statusId      uint64
}

func (self *JobStatus) updateKeybindings() {
	k := &self.Keys
	switch self.filterState {
	case Filtering:
		k.SetEnabled(false)
		k.CancelWhileFiltering.SetEnabled(true)
		k.AcceptWhileFiltering.SetEnabled(self.FilterInput.Value() != "")
		self.setViewportKeysEnabled(false)

	default:
		k.SetEnabled(true)
		k.Signal.SetEnabled(self.canSignal != "")
		k.ClearFilter.SetEnabled(self.filterState == FilterApplied)
		k.Back.SetEnabled(self.filterState == Unfiltered)
		k.CancelWhileFiltering.SetEnabled(false)
		k.AcceptWhileFiltering.SetEnabled(false)
		self.setViewportKeysEnabled(true)
	}
}

func (self *JobStatus) setViewportKeysEnabled(v bool) {
	k := &self.viewport.KeyMap
	k.PageDown.SetEnabled(v)
	k.PageUp.SetEnabled(v)
	k.HalfPageDown.SetEnabled(v)
	k.HalfPageUp.SetEnabled(v)
	k.Down.SetEnabled(v)
	k.Up.SetEnabled(v)
}

func (self *JobStatus) init() *JobStatus {
	height, ypos := self.viewportHeight()
	self.viewport = viewport.New(self.width, height)
	self.viewport.YPosition = ypos
	self.viewport.Style = self.Styles.View
	self.updateContent()
	self.ready = true
	return self
}

func (self *JobStatus) viewportHeight() (int, int) {
	headerHeight := lipgloss.Height(self.headerView())
	footerHeight := lipgloss.Height(self.footerView())
	return self.height - headerHeight - footerHeight, headerHeight
}

func (self *JobStatus) WithBackTo(fn func()) *JobStatus {
	self.backToFunc = fn
	return self
}

func (self *JobStatus) Update(msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return self.handleKeys(msg)
	case tea.WindowSizeMsg:
		return self.handleWindowSize(msg)
	case statusMessageTimeoutMsg:
		self.hideStatusMessage(msg.id)
		return nil
	}

	if cmd, ok := self.updateState(msg); ok {
		return cmd
	}

	var cmd tea.Cmd
	self.viewport, cmd = self.viewport.Update(msg)
	return cmd
}

func (self *JobStatus) handleKeys(msg tea.KeyMsg) tea.Cmd {
	if key.Matches(msg, self.Keys.ForceQuit) {
		return tea.Quit
	} else if cmd, ok := self.updateState(msg); ok {
		return cmd
	}

	if self.filterState == Filtering {
		return self.handleFiltering(msg)
	}
	return self.handleLocalKeys(msg)
}

func (self *JobStatus) updateState(msg tea.Msg) (tea.Cmd, bool) {
	if self.state == stateJobSignal {
		return self.signalYesNo.Update(msg), true
	}
	return nil, false
}

func (self *JobStatus) handleFiltering(msg tea.KeyMsg) tea.Cmd {
	k := &self.Keys
	switch {
	case key.Matches(msg, k.CancelWhileFiltering):
		return self.resetFiltering()

	case key.Matches(msg, k.AcceptWhileFiltering):
		self.filterState = FilterApplied
		self.FilterInput.Blur()
		self.updateKeybindings()
		self.render.SetFilter(self.FilterInput.Value())
		self.updateContent()
		return nil
	}

	filterInput, cmd := self.FilterInput.Update(msg)
	filterChanged := self.FilterInput.Value() != filterInput.Value()
	self.FilterInput = filterInput
	if filterChanged {
		k.AcceptWhileFiltering.SetEnabled(self.FilterInput.Value() != "")
		if self.FilterInput.Value() != "" {
			self.render.SetFilter(self.FilterInput.Value())
		} else {
			self.render.ResetFilter()
		}
		self.updateContent()
	}
	return cmd
}

func (self *JobStatus) handleLocalKeys(msg tea.KeyMsg) tea.Cmd {
	k := &self.Keys
	switch {
	case key.Matches(msg, k.GoToStart):
		self.viewport.GotoTop()
		return nil

	case key.Matches(msg, k.GoToEnd):
		self.viewport.GotoBottom()
		return nil

	case key.Matches(msg, k.Filter):
		return self.initFiltering()

	case key.Matches(msg, k.ClearFilter):
		return self.resetFiltering()

	case key.Matches(msg, k.Jump):
		return self.jumpToNextSection()

	case key.Matches(msg, k.Signal):
		self.confirmSignal(self.canSignal)
		return nil

	case key.Matches(msg, k.Back):
		if self.backToFunc == nil {
			return tea.Quit
		}
		self.backToFunc()
		return nil

	case key.Matches(msg, k.Quit):
		return tea.Quit

	case key.Matches(msg, k.ShowFullHelp, k.CloseFullHelp):
		self.Help.ShowAll = !self.Help.ShowAll
		self.heightChanged = true
		return nil
	}

	var cmd tea.Cmd
	self.viewport, cmd = self.viewport.Update(msg)
	return cmd
}

func (self *JobStatus) initFiltering() tea.Cmd {
	self.filterState = Filtering
	self.updateKeybindings()
	self.FilterInput.CursorEnd()
	self.FilterInput.Focus()
	return textinput.Blink
}

func (self *JobStatus) resetFiltering() tea.Cmd {
	self.filterState = Unfiltered
	self.updateKeybindings()
	self.FilterInput.Reset()
	self.render.ResetFilter()
	self.updateContent()
	return nil
}

func (self *JobStatus) handleWindowSize(msg tea.WindowSizeMsg) tea.Cmd {
	self.width, self.height = msg.Width, msg.Height
	s := &self.Styles
	self.Help.Width = self.width - s.Help.GetHorizontalFrameSize()

	self.viewport.Width = self.width
	height, _ := self.viewportHeight()
	self.viewport.Height = height

	if cmd, ok := self.updateState(msg); ok {
		return cmd
	}
	return nil
}

func (self *JobStatus) View() string {
	if self.state == stateJobSignal {
		return self.signalYesNo.View()
	}

	header := self.headerView()
	footer := self.footerView()
	self.viewport.Height = self.height - lipgloss.Height(header) -
		lipgloss.Height(footer)
	if self.heightChanged {
		footer = self.footerView()
	}
	return lipgloss.JoinVertical(lipgloss.Left,
		header, self.viewport.View(), footer)
}

func (self *JobStatus) headerView() string {
	var sb strings.Builder
	if self.filterState == Filtering {
		sb.WriteString(self.FilterInput.View())
	} else {
		self.renderTitle(&sb)
	}

	s := &self.Styles
	width := self.width - s.TitleBar.GetHorizontalFrameSize()
	return truncate.StringWithTail(s.TitleBar.Render(sb.String()),
		uint(width), ellipsis)
}

//nolint:errcheck // I don't expect errors from sb
func (self *JobStatus) renderTitle(sb io.StringWriter) {
	s := &self.Styles
	sb.WriteString(s.Title.Render(self.name))

	if self.job != nil {
		self.renderJobTime(sb)
	}

	if self.filterState == FilterApplied {
		sb.WriteString(s.StatusBar.Render(fmt.Sprintf(" %s “%s”",
			eyes, self.FilterInput.Value())))
	}

	if self.statusMessage != "" {
		sb.WriteString(fmt.Sprintf(" %s %s",
			s.DividerDot, self.statusMessage))
	}
}

//nolint:errcheck // I don't expect errors from sb
func (self *JobStatus) renderJobTime(sb io.StringWriter) {
	if self.viewport.YOffset <= self.render.JobTimeLine() {
		return
	}

	s := &self.Styles
	if d, ok := self.job.Running(); ok {
		sb.WriteString(" " + runner)
		sb.WriteString(s.StatusBar.Render(d.Truncate(time.Second).String()))
	} else if t := self.job.SleepingUntil(); !t.IsZero() {
		sb.WriteString(" " + sleeping)
		sb.WriteString(s.StatusBar.Render(
			time.Until(t).Truncate(time.Second).String()))
	}
}

func (self *JobStatus) footerView() string {
	return lipgloss.JoinVertical(lipgloss.Left, self.paginationView(),
		self.helpView())
}

func (self *JobStatus) paginationView() string {
	var sb strings.Builder
	s := &self.Styles
	scrollPercent := self.scrollPercent()
	sb.WriteString(s.ArabicPagination.Render(fmt.Sprintf(
		"%.f%%", scrollPercent*100)))

	w := s.Pagination.GetHorizontalFrameSize()
	maxPages := self.width - w - lipgloss.Width(sb.String())
	pages := min(self.pages(), maxPages)

	activeDotsN := 1
	if pages > 1 {
		activeDotsN = int(scrollPercent * float64(pages))
	}

	if activeDotsN > 0 {
		sb.WriteString(strings.Repeat(s.ActivePaginationDot.Render(),
			activeDotsN))
	}
	if n := pages - activeDotsN; n > 0 {
		sb.WriteString(strings.Repeat(s.InactivePaginationDot.Render(), n))
	}
	return s.Pagination.Render(sb.String())
}

func (self *JobStatus) scrollPercent() float64 {
	totalLines := self.viewport.TotalLineCount()
	height := self.viewport.Height
	if totalLines <= height {
		return 1.0
	}
	return min(1.0, float64(self.viewport.YOffset)/float64(totalLines-height))
}

func (self *JobStatus) pages() int {
	height := self.viewport.Height
	if height == 0 {
		return 1
	}

	lines := self.viewport.TotalLineCount()
	pages := lines / height
	if lines%height > 0 {
		pages++
	}
	return pages
}

func (self *JobStatus) helpView() string {
	return self.Styles.Help.Render(self.Help.View(self))
}

func (self *JobStatus) ShortHelp() []key.Binding {
	return []key.Binding{
		self.viewport.KeyMap.Down,
		self.viewport.KeyMap.Up,

		self.Keys.Jump,
		self.Keys.Filter,
		self.Keys.ClearFilter,
		self.Keys.AcceptWhileFiltering,
		self.Keys.CancelWhileFiltering,

		self.Keys.Signal,
		self.Keys.Back,

		self.Keys.Quit,
		self.Keys.ShowFullHelp,
	}
}

func (self *JobStatus) FullHelp() [][]key.Binding {
	return [][]key.Binding{
		{
			self.viewport.KeyMap.Down,
			self.viewport.KeyMap.Up,
			self.viewport.KeyMap.PageDown,
			self.viewport.KeyMap.PageUp,
		},
		{
			self.Keys.GoToStart,
			self.Keys.GoToEnd,
			self.viewport.KeyMap.HalfPageDown,
			self.viewport.KeyMap.HalfPageUp,
		},
		{
			self.Keys.Jump,
			self.Keys.Filter,
			self.Keys.ClearFilter,
			self.Keys.AcceptWhileFiltering,
			self.Keys.CancelWhileFiltering,
			self.Keys.Signal,
		},
		{
			self.Keys.Back,
			self.Keys.Quit,
			self.Keys.CloseFullHelp,
		},
	}
}

func (self *JobStatus) SetJob(name string, job *job.Status) {
	self.name, self.job = name, job
	self.render.SetJob(job)

	self.canSignal = self.job.CanSignal()
	if self.filterState != Filtering {
		self.Keys.Signal.SetEnabled(self.canSignal != "")
	}

	if !self.ready {
		self.init()
	} else {
		self.updateContent()
	}
}

func (self *JobStatus) updateContent() {
	self.viewport.SetContent(strings.TrimRight(self.render.View(), "\n"))
	if self.filterState != Filtering {
		self.Keys.Jump.SetEnabled(len(self.render.JumpLines()) > 0 &&
			self.viewport.TotalLineCount() > self.viewport.VisibleLineCount())
	}
}

func (self *JobStatus) Reset() {
	self.job = nil
	self.canSignal = ""
	self.render.Reset()
	self.viewport.SetContent("")
	self.viewport.SetYOffset(0)
}

func (self *JobStatus) confirmSignal(name string) {
	sigTitle := titler.String(name)

	items := []ListItem{
		{
			Caption: "No",
			Desc:    "Just do nothing and go back",
			Func: func() tea.Cmd {
				self.switchToView()
				return nil
			},
		},
		{
			Caption: "Yes",
			Desc:    fmt.Sprintf("%s %s and go back", sigTitle, self.name),
			Func:    func() tea.Cmd { return self.signal(name) },
		},
	}
	title := fmt.Sprintf("%s %s?", sigTitle, self.name)

	self.signalYesNo = NewSimpleList(items, self.width, self.height, title).
		WithBackTo(self.switchToView)
	self.state = stateJobSignal
}

func (self *JobStatus) switchToView() {
	self.state = stateJobView
	self.signalYesNo = nil
}

func (self *JobStatus) signal(name string) tea.Cmd {
	self.switchToView()
	if name != self.canSignal {
		return self.NewStatusMessage(fmt.Sprintf(
			"Inconsistent signal %q", name))
	}

	switch name {
	case "reset":
		return self.resetCmd()
	case "wakeup":
		return self.wakeupCmd()
	}
	return nil
}

func (self *JobStatus) resetCmd() tea.Cmd {
	return tea.Sequence(self.NewStatusMessage("Send reset signal"),
		func() tea.Msg { return self.client.SignalReset(self.name) })
}

func (self *JobStatus) wakeupCmd() tea.Cmd {
	return tea.Sequence(self.NewStatusMessage("Send wakeup signal"),
		func() tea.Msg { return self.client.SignalWakeup(self.name) })
}

func (self *JobStatus) NewStatusMessage(s string) tea.Cmd {
	self.statusId++
	self.statusMessage = s
	return tea.Tick(self.StatusMessageLifetime,
		func(_ time.Time) tea.Msg {
			return statusMessageTimeoutMsg{self.statusId}
		})
}

type statusMessageTimeoutMsg struct {
	id uint64
}

func (self *JobStatus) hideStatusMessage(id uint64) {
	if self.statusId == id {
		self.statusMessage = ""
	}
}

func (self *JobStatus) jumpToNextSection() tea.Cmd {
	jumpLines := self.render.JumpLines()
	if len(jumpLines) == 0 ||
		self.viewport.VisibleLineCount() == self.viewport.TotalLineCount() {
		return nil
	} else if self.viewport.AtBottom() {
		self.viewport.SetYOffset(0)
		return nil
	}

	maxLine := self.viewport.TotalLineCount() - self.viewport.Height
	for i := 0; i < len(jumpLines); i++ {
		if nextLine := jumpLines[i]; nextLine > self.viewport.YOffset &&
			nextLine <= maxLine {
			self.viewport.SetYOffset(nextLine)
			return nil
		}
	}
	self.viewport.SetYOffset(0)
	return nil
}
