package hooks

import (
	"context"
	"fmt"
	"maps"
	"math"
	"strings"
	"time"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/daemon/filters"
)

const (
	EnvType     = "ZREPL_HOOKTYPE"
	EnvDryRun   = "ZREPL_DRYRUN"
	EnvFS       = "ZREPL_FS"
	EnvSnapshot = "ZREPL_SNAPNAME"
	EnvTimeout  = "ZREPL_TIMEOUT"
)

func NewCommandHook(in *config.HookCommand) (*CommandHook, error) {
	r := &CommandHook{
		errIsFatal: in.ErrIsFatal,
		command:    in.Path,
		timeout:    in.Timeout,

		args: in.Args,
		env:  in.Env,
	}

	filter, err := filters.NewFromConfig(in.Filesystems, in.Datasets)
	if err != nil {
		return nil, fmt.Errorf("cannot parse filesystem filter: %w", err)
	}
	r.filter = filter
	return r, nil
}

type CommandHook struct {
	filter     *filters.DatasetFilter
	errIsFatal bool
	command    string
	timeout    time.Duration

	args []string
	env  map[string]string

	combinedOutput bool
}

func (self *CommandHook) WithCombinedOutput() *CommandHook {
	self.combinedOutput = true
	return self
}

func (self *CommandHook) Filesystems() *filters.DatasetFilter {
	return self.filter
}

func (self *CommandHook) ErrIsFatal() bool { return self.errIsFatal }

func (self *CommandHook) String() string { return self.command }

func (self *CommandHook) Run(ctx context.Context, edge Edge, phase Phase,
	dryRun bool, extra map[string]string,
) HookReport {
	report := &CommandHookReport{
		Command: self.command,
		Env:     self.makeEnv(edge, phase, dryRun, extra),
		// no report.Args
	}

	cmd := NewCommand(self.command, self.args...).
		WithEnv(report.Env).
		WithTimeout(self.timeout)

	report.Err = cmd.Run(ctx)
	if self.combinedOutput {
		report.CombinedOutput = cmd.CombinedOutput()
	}
	return report
}

func (self *CommandHook) makeEnv(edge Edge, phase Phase, dryRun bool,
	extra map[string]string,
) map[string]string {
	hookEnv := self.hookEnv(edge, phase, dryRun)
	env := make(map[string]string, len(self.env)+len(hookEnv)+len(extra))
	maps.Copy(env, self.env)
	maps.Copy(env, hookEnv)
	maps.Copy(env, extra)
	return env
}

func (self *CommandHook) hookEnv(edge Edge, phase Phase, dryRun bool,
) map[string]string {
	env := make(map[string]string, 3)
	env[EnvTimeout] = fmt.Sprintf("%.f", math.Floor(self.timeout.Seconds()))
	env[EnvType] = strings.ToLower(edge.StringForPhase(phase))

	if dryRun {
		env[EnvDryRun] = "true"
	} else {
		env[EnvDryRun] = ""
	}
	return env
}
