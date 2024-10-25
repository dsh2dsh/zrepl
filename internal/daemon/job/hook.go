package job

import (
	"context"
	"time"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/daemon/hooks"
)

const (
	envJobName = "ZREPL_JOB_NAME"
	envJobErr  = "ZREPL_JOB_ERR"
)

func NewHookFromConfig(in *config.HookCommand) *Hook {
	return &Hook{
		path: in.Path,
		args: in.Args,
		env:  in.Env,

		timeout:    in.Timeout,
		errIsFatal: in.ErrIsFatal,
	}
}

type Hook struct {
	path string
	args []string
	env  map[string]string

	timeout    time.Duration
	errIsFatal bool
	postHook   bool
}

func (self *Hook) WithPostHook(v bool) *Hook {
	self.postHook = v
	return self
}

func (self *Hook) ErrIsFatal() bool { return self.errIsFatal }

func (self *Hook) Run(ctx context.Context, j Job) error {
	cmd := hooks.NewCommand(self.path, self.args...).
		WithEnv(self.env, self.makeJobEnv(j)).
		WithTimeout(self.timeout)
	return cmd.Run(ctx)
}

func (self *Hook) makeJobEnv(j Job) map[string]string {
	var jobErr string
	if self.postHook {
		if jobStatus := j.Status(); jobStatus != nil {
			jobErr = jobStatus.Error()
		}
	}
	env := map[string]string{
		envJobName: j.Name(),
		envJobErr:  jobErr,
	}
	return env
}
