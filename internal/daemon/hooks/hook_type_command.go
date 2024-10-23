package hooks

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/daemon/filters"
	"github.com/dsh2dsh/zrepl/internal/logger"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

const (
	EnvType     = "ZREPL_HOOKTYPE"
	EnvDryRun   = "ZREPL_DRYRUN"
	EnvFS       = "ZREPL_FS"
	EnvSnapshot = "ZREPL_SNAPNAME"
	EnvTimeout  = "ZREPL_TIMEOUT"
)

func NewHookEnv(edge Edge, phase Phase, dryRun bool, timeout time.Duration,
	extra map[string]string,
) map[string]string {
	r := map[string]string{
		EnvTimeout: fmt.Sprintf("%.f", math.Floor(timeout.Seconds())),
	}

	edgeString := edge.StringForPhase(phase)
	r[EnvType] = strings.ToLower(edgeString)

	var dryRunString string
	if dryRun {
		dryRunString = "true"
	} else {
		dryRunString = ""
	}
	r[EnvDryRun] = dryRunString

	for k, v := range extra {
		r[k] = v
	}
	return r
}

func NewCommandHook(in *config.HookCommand) (*CommandHook, error) {
	r := &CommandHook{
		errIsFatal: in.ErrIsFatal,
		command:    in.Path,
		timeout:    in.Timeout,
	}

	filter, err := filters.NewFromConfig(in.Filesystems, in.Datasets)
	if err != nil {
		return nil, fmt.Errorf("cannot parse filesystem filter: %s", err)
	}
	r.filter = filter
	return r, nil
}

type CommandHook struct {
	filter     zfs.DatasetFilter
	errIsFatal bool
	command    string
	timeout    time.Duration

	combinedOutput bool
}

func (self *CommandHook) WithCombinedOutput() *CommandHook {
	self.combinedOutput = true
	return self
}

func (self *CommandHook) Filesystems() zfs.DatasetFilter { return self.filter }

func (self *CommandHook) ErrIsFatal() bool { return self.errIsFatal }

func (self *CommandHook) String() string { return self.command }

func (self *CommandHook) Run(ctx context.Context, edge Edge, phase Phase,
	dryRun bool, extra map[string]string,
) HookReport {
	if self.timeout > 0 {
		cmdCtx, cancel := context.WithTimeout(ctx, self.timeout)
		defer cancel()
		ctx = cmdCtx
	}

	cmd := exec.CommandContext(ctx, self.command)
	env, hookEnv := self.makeEnv(edge, phase, dryRun, extra)
	cmd.Env = env

	report := &CommandHookReport{
		Command: self.command,
		Env:     hookEnv,
		// no report.Args
	}

	combinedOutput, err := cmd.Output()
	l := getLogger(ctx).WithField("command", self.command)
	logOutput(l, logger.Info, "stdout", combinedOutput)

	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			logOutput(l, logger.Warn, "stderr", ee.Stderr)
			if self.combinedOutput {
				combinedOutput = slices.Concat(combinedOutput, ee.Stderr)
			}
		}
		if errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
			err = fmt.Errorf("timed out after %s: %s", self.timeout, err)
		}
		report.Err = err
	}

	if self.combinedOutput {
		report.CombinedOutput = combinedOutput
	}
	return report
}

func (self *CommandHook) makeEnv(edge Edge, phase Phase, dryRun bool,
	extra map[string]string,
) ([]string, map[string]string) {
	env := slices.Clone(os.Environ())
	hookEnv := NewHookEnv(edge, phase, dryRun, self.timeout, extra)
	env = slices.Grow(env, len(hookEnv))
	for k, v := range hookEnv {
		env = append(env, k+"="+v)
	}
	return env, hookEnv
}
