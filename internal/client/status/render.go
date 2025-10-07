package status

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/lipgloss"
	"go.yaml.in/yaml/v4"

	"github.com/dsh2dsh/zrepl/internal/daemon/job"
)

func DefaultRenderStyles() (s RenderStyles) {
	verySubduedColor := lipgloss.AdaptiveColor{Light: "#DDDADA", Dark: "#3C3C3C"}
	subduedColor := lipgloss.AdaptiveColor{Light: "#9B9B9B", Dark: "#5C5C5C"}

	s.Title = lipgloss.NewStyle()
	s.Content = lipgloss.NewStyle().MarginLeft(2)
	s.Indent = lipgloss.NewStyle().MarginLeft(2)

	s.NotYet = lipgloss.NewStyle().SetString("...")

	s.InactiveFsIcon = lipgloss.NewStyle().Width(2)
	s.RunningFsIcon = s.InactiveFsIcon.SetString(runner)

	s.InactiveFs = lipgloss.NewStyle().
		Foreground(lipgloss.AdaptiveColor{Light: "#A49FA5", Dark: "#777777"})
	s.RunningFs = lipgloss.NewStyle()
	s.FsNext = lipgloss.NewStyle().MarginLeft(4)

	s.FilterMatch = lipgloss.NewStyle().Underline(true)

	s.StatusBar = lipgloss.NewStyle().
		Foreground(lipgloss.AdaptiveColor{Light: "#A49FA5", Dark: "#777777"}).
		PaddingLeft(2)

	s.StatusEmpty = lipgloss.NewStyle().Foreground(subduedColor)

	s.StatusBarFilterCount = lipgloss.NewStyle().Foreground(verySubduedColor)

	s.DividerDot = lipgloss.NewStyle().
		Foreground(verySubduedColor).
		SetString(" " + bullet + " ")

	s.SnapState = lipgloss.NewStyle()
	s.SnapTime = lipgloss.NewStyle().Align(lipgloss.Right)
	return s
}

type RenderStyles struct {
	Title   lipgloss.Style
	Content lipgloss.Style
	Indent  lipgloss.Style

	NotYet lipgloss.Style

	InactiveFsIcon lipgloss.Style
	RunningFsIcon  lipgloss.Style

	InactiveFs lipgloss.Style
	RunningFs  lipgloss.Style
	FsNext     lipgloss.Style

	FilterMatch          lipgloss.Style
	StatusBar            lipgloss.Style
	StatusEmpty          lipgloss.Style
	StatusBarFilterCount lipgloss.Style
	DividerDot           lipgloss.Style

	SnapState lipgloss.Style
	SnapTime  lipgloss.Style
}

func (self *RenderStyles) Filesystem(running bool) (lipgloss.Style,
	lipgloss.Style,
) {
	if running {
		return self.RunningFs, self.RunningFsIcon
	}
	return self.InactiveFs, self.InactiveFsIcon
}

// --------------------------------------------------

func NewJobRender() *JobRender {
	return &JobRender{
		Styles: DefaultRenderStyles(),

		jumpLines: make([]int, 0, 4),

		bar: progress.New(),
	}
}

type JobRender struct {
	Styles RenderStyles

	job *job.Status
	b   bytes.Buffer

	currentLine int
	jobTimeLine int
	jumpLines   []int

	filterState FilterState
	filterValue string

	bar   progress.Model
	speed speed
}

func (self *JobRender) SetJob(job *job.Status) {
	self.job = job
}

func (self *JobRender) Reset() {
	self.job = nil
	self.jumpLines = self.jumpLines[:0]
	self.speed.Reset()
}

func (self *JobRender) View() string {
	self.currentLine = 0
	self.jumpLines = self.jumpLines[:0]

	defer self.b.Reset()
	self.viewType()

	switch j := self.job.JobSpecific.(type) {
	case *job.ActiveSideStatus:
		self.viewActiveStatus(j)
	case *job.SnapJobStatus:
		self.renderSnap(j.Snapshotting)
		self.renderPruning("Pruning snapshots:", j.Pruning)
	case *job.PassiveStatus:
		if self.job.Type == job.TypeSource {
			self.renderSnap(j.Snapper)
		} else {
			self.viewUnknown()
		}
	default:
		self.viewUnknown()
	}
	return self.b.String()
}

func (self *JobRender) viewType() {
	defer self.sectionEnd()
	self.printLn("Type: " + string(self.job.Type))
	if cron := self.job.Cron(); cron != "" {
		self.printLn("Interval: " + cron)
	}

	self.jobTimeLine = self.currentLine
	if t, ok := self.job.Running(); ok {
		self.printLn("Running: " + t.Truncate(time.Second).String())
	} else if t := self.job.SleepingUntil(); !t.IsZero() {
		self.printLn(fmt.Sprintf("Sleep until: %s (%s remaining)",
			t, time.Until(t).Truncate(time.Second)))
	}

	if err := self.job.Error(); err != "" {
		self.printLn("Last error: " + err)
	}
}

func (self *JobRender) sectionEnd() {
	self.newline()
}

func (self *JobRender) printLn(s string) {
	self.b.WriteString(s)
	self.currentLine += strings.Count(s, "\n")
	self.newline()
}

func (self *JobRender) newline() {
	self.b.WriteByte('\n')
	self.currentLine++
}

func (self *JobRender) JobTimeLine() int { return self.jobTimeLine }

func (self *JobRender) JumpLines() []int { return self.jumpLines }

func (self *JobRender) viewActiveStatus(j *job.ActiveSideStatus) {
	self.viewReplication(j.Replication)
	self.renderPruning("Pruning Sender:", j.PruningSender)
	self.renderPruning("Pruning Receiver:", j.PruningReceiver)
	if self.job.Type == job.TypePush {
		self.renderSnap(j.Snapshotting)
	}
}

func (self *JobRender) sectionWithTitle(title string) func() {
	self.jumpLines = append(self.jumpLines, self.currentLine)
	self.printLn(self.Styles.Title.Render(title))
	return self.sectionEnd
}

func (self *JobRender) indentMultiline(s string, style lipgloss.Style) string {
	before, after, found := strings.Cut(s, "\n")
	if !found {
		return s
	}
	return before + "\n" + style.Render(after)
}

func (self *JobRender) viewUnknown() {
	self.printLn("No status view for this job type, dumping as YAML.")
	self.newline()

	b, err := yaml.Marshal(self.job.JobSpecific)
	if err != nil {
		self.printLn(fmt.Errorf("marshaling job status to YAML: %w", err).Error())
	} else {
		self.printLn(string(b))
	}
}

func (self *JobRender) SetFilter(value string) {
	self.filterState, self.filterValue = FilterApplied, value
}

func (self *JobRender) ResetFilter() {
	self.filterState, self.filterValue = Unfiltered, ""
}
