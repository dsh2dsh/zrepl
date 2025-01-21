package snapper

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dsh2dsh/zrepl/internal/daemon/hooks"
)

var planErr = errors.New("end run job plan with error")

func NewProgress() *progress {
	return &progress{state: SnapPending}
}

// All fields protected by Snapper.mtx
type progress struct {
	state SnapState
	mu    sync.Mutex

	// SnapStarted, SnapDone, SnapError
	name     string
	startAt  time.Time
	hookPlan *hooks.Plan

	// SnapDone
	doneAt time.Time

	// SnapErr TODO disambiguate state
	runResults hooks.PlanReport
}

func (self *progress) CreateSnapshot(ctx context.Context, dryRun bool,
	snapName string, hookPlan *hooks.Plan,
) error {
	self.start(snapName, hookPlan)
	l := getLogger(ctx)
	l.With(slog.String("report", hookPlan.Report().String())).
		Debug("begin run job plan")

	hookPlan.Run(ctx, dryRun)
	hookPlanReport := hookPlan.Report()

	l = l.With(slog.String("report", hookPlanReport.String()))
	if hookPlanReport.HadError() { // not just fatal errors
		l.Error(planErr.Error())
		self.StateError()
		return planErr
	}

	l.Info("end run job plan successful")
	self.done(hookPlanReport)
	return nil
}

func (self *progress) start(name string, plan *hooks.Plan) {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.name = name
	self.startAt = time.Now()
	self.hookPlan = plan
	self.state = SnapStarted
}

func (self *progress) done(report hooks.PlanReport) {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.doneAt = time.Now()
	self.state = SnapDone
	self.runResults = report
}

func (self *progress) StateError() {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.doneAt = time.Now()
	self.state = SnapError
}

func (self *progress) Report(fs string) *ReportFilesystem {
	self.mu.Lock()
	defer self.mu.Unlock()

	hooksStr, hooksHadError := self.buildReport()
	return &ReportFilesystem{
		Path:          fs,
		State:         self.state,
		SnapName:      self.name,
		StartAt:       self.startAt,
		DoneAt:        self.doneAt,
		Hooks:         hooksStr,
		HooksHadError: hooksHadError,
	}
}

type ReportFilesystem struct {
	Path  string
	State SnapState

	// Valid in SnapStarted and later
	SnapName      string
	StartAt       time.Time
	Hooks         string
	HooksHadError bool

	// Valid in SnapDone | SnapError
	DoneAt time.Time
}

func (self *progress) buildReport() (string, bool) {
	if self.hookPlan == nil {
		return "", false
	}

	hr := self.hookPlan.Report()
	// FIXME: technically this belongs into client
	// but we can't serialize hooks.Step ATM
	rightPad := func(str string, length int, pad string) string {
		if len(str) > length {
			return str[:length]
		}
		return str + strings.Repeat(pad, length-len(str))
	}

	hooksHadError := hr.HadError()
	rows := make([][]string, len(hr))
	const numCols = 4
	lens := make([]int, numCols)

	for i, e := range hr {
		rows[i] = make([]string, numCols)
		rows[i][0] = strconv.Itoa(i + 1)
		rows[i][1] = e.Status.String()
		runTime := "..."
		if e.Status != hooks.StepPending {
			runTime = e.End.Sub(e.Begin).Round(time.Millisecond).String()
		}
		rows[i][2] = runTime
		rows[i][3] = ""
		if e.Report != nil {
			rows[i][3] = e.Report.String()
		}
		for j, col := range lens {
			if len(rows[i][j]) > col {
				lens[j] = len(rows[i][j])
			}
		}
	}

	rowsFlat := make([]string, len(hr))
	for i, r := range rows {
		colsPadded := make([]string, len(r))
		for j, c := range r[:len(r)-1] {
			colsPadded[j] = rightPad(c, lens[j], " ")
		}
		colsPadded[len(r)-1] = r[len(r)-1]
		rowsFlat[i] = strings.Join(colsPadded, " ")
	}

	hooksStr := strings.Join(rowsFlat, "\n")
	return hooksStr, hooksHadError
}
