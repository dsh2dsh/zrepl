package hooks

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/dsh2dsh/zrepl/internal/logger"
)

type Hook interface {
	String() string

	// If true and the Pre edge invocation of Run fails, Post edge will not run
	// and other Pre edges will not run.
	ErrIsFatal() bool

	// Run is invoked by HookPlan for a Pre edge. If HookReport.HadError() ==
	// false, the Post edge will be invoked, too.
	Run(ctx context.Context, edge Edge, phase Phase, dryRun bool,
		extra map[string]string,
	) HookReport
}

type Phase string

const (
	PhaseSnapshot = Phase("snapshot")
	PhaseTesting  = Phase("testing")
)

func (p Phase) String() string { return string(p) }

//go:generate stringer -type=Edge
type Edge uint

const (
	Pre = Edge(1 << iota)
	Callback
	Post
)

func (e Edge) StringForPhase(phase Phase) string {
	return fmt.Sprintf("%s_%s", e.String(), phase.String())
}

//go:generate enumer -type=StepStatus -trimprefix=Step
type StepStatus int

const (
	StepPending StepStatus = 1 << iota
	StepExec
	StepOk
	StepErr
	StepSkippedDueToFatalErr
	StepSkippedDueToPreErr
)

type HookReport interface {
	String() string
	HadError() bool
	Error() string
}

type Step struct {
	Hook       Hook
	Edge       Edge
	Status     StepStatus
	Begin, End time.Time
	// Report may be nil
	//
	// FIXME cannot serialize this for client status, but contains interesting
	// info (like what error happened)
	Report HookReport
}

func (s Step) String() (out string) {
	fatal := "~"
	if s.Hook.ErrIsFatal() && s.Edge == Pre {
		fatal = "!"
	}
	runTime := "..."
	if s.Status != StepPending {
		t := s.End.Sub(s.Begin)
		runTime = t.Round(time.Millisecond).String()
	}
	return fmt.Sprintf("[%s] [%5s] %s [%s]  %s",
		s.Status, runTime, fatal, s.Edge, s.Hook)
}

type Plan struct {
	mtx sync.RWMutex

	steps []*Step
	pre   []*Step // protected by mtx
	cb    *Step
	// not reversed, i.e. entry at index i corresponds to pre-edge in pre[i]
	post []*Step

	phase Phase
	env   map[string]string
}

func NewPlan(hooks List, phase Phase, cb *CallbackHook, extra map[string]string,
) (*Plan, error) {
	pre := make([]*Step, 0, len(hooks))
	post := make([]*Step, 0, len(hooks))
	// TODO sanity check unique name of hook?
	for _, hook := range hooks {
		pre = append(pre, &Step{
			Hook:   hook,
			Edge:   Pre,
			Status: StepPending,
		})
		post = append(post, &Step{
			Hook:   hook,
			Edge:   Post,
			Status: StepPending,
		})
	}

	cbE := &Step{
		Hook:   cb,
		Edge:   Callback,
		Status: StepPending,
	}

	steps := make([]*Step, 0, len(pre)+len(post)+1)
	steps = append(steps, pre...)
	steps = append(steps, cbE)
	for i := len(post) - 1; i >= 0; i-- {
		steps = append(steps, post[i])
	}

	plan := &Plan{
		phase: phase,
		env:   extra,
		steps: steps,
		pre:   pre,
		post:  post,
		cb:    cbE,
	}
	return plan, nil
}

type PlanReport []Step

func (p *Plan) Report() PlanReport {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	rep := make([]Step, len(p.steps))
	for i := range rep {
		rep[i] = *p.steps[i]
	}
	return rep
}

func (r PlanReport) HadError() bool {
	for _, e := range r {
		if e.Status == StepErr {
			return true
		}
	}
	return false
}

func (r PlanReport) HadFatalError() bool {
	for _, e := range r {
		if e.Status == StepSkippedDueToFatalErr {
			return true
		}
	}
	return false
}

func (r PlanReport) String() string {
	stepStrings := make([]string, len(r))
	for i, e := range r {
		stepStrings[i] = fmt.Sprintf("%02d %s", i+1, e)
	}
	return strings.Join(stepStrings, "\n")
}

func (p *Plan) Run(ctx context.Context, dryRun bool) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	w := func(f func()) {
		p.mtx.RUnlock()
		defer p.mtx.RLock()
		p.mtx.Lock()
		defer p.mtx.Unlock()
		f()
	}

	runHook := func(s *Step, ctx context.Context, edge Edge) HookReport {
		w(func() { s.Status = StepExec })
		begin := time.Now()
		r := s.Hook.Run(ctx, edge, p.phase, dryRun, p.env)
		end := time.Now()
		w(func() {
			s.Report = r
			s.Status = StepOk
			if r.HadError() {
				s.Status = StepErr
			}
			s.Begin, s.End = begin, end
		})
		return r
	}

	l := getLogger(ctx)
	// it's a stack, execute until we reach the end of the list (last item in)
	// or fail between
	l.Info("run pre-edges in configuration order")
	next := 0
	for ; next < len(p.pre); next++ {
		e := p.pre[next]
		l := l.With(slog.String("hook", e.Hook.String()))
		r := runHook(e, ctx, Pre)
		if r.HadError() {
			logger.WithError(l, r, "hook invocation failed for pre-edge")
			if e.Hook.ErrIsFatal() {
				l.Error("the hook run was aborted due to a fatal error in this hook")
				break
			}
		}
	}

	if next != len(p.pre) {
		l.Error("fatal error in a pre-snapshot hook invocation")
		l.Error("no snapshot will be taken")
		l.Error("only running post-edges for successful pre-edges")
		w(func() {
			p.post[next].Status = StepSkippedDueToFatalErr
			for i := next + 1; i < len(p.pre); i++ {
				p.pre[i].Status = StepSkippedDueToFatalErr
				p.post[i].Status = StepSkippedDueToFatalErr
			}
			p.cb.Status = StepSkippedDueToFatalErr
		})
		return
	}

	l.Info("running callback")
	if cbR := runHook(p.cb, ctx, Callback); cbR.HadError() {
		logger.WithError(l, cbR, "callback failed")
	}

	l.Info("run post-edges for successful pre-edges in reverse configuration order")
	// the constructor produces pre and post entries
	// post is NOT reversed
	next-- // now at index of last executed pre-edge
	for ; next >= 0; next-- {
		e := p.post[next]
		l := l.With(slog.String("hook", e.Hook.String()))
		if p.pre[next].Status != StepOk {
			if p.pre[next].Status != StepErr {
				panic(fmt.Sprintf(
					"expecting a pre-edge hook report to be either Ok or Err, got %s",
					p.pre[next].Status))
			}
			l.Info("skip post-edge because pre-edge failed")
			w(func() {
				e.Status = StepSkippedDueToPreErr
			})
			continue
		}

		report := runHook(e, ctx, Post)
		if report.HadError() {
			logger.WithError(l, report, "hook invocation failed for post-edge")
			l.Error("subsequent post-edges run regardless of this post-edge failure")
		}
		// ErrIsFatal is only relevant for Pre
	}
}
