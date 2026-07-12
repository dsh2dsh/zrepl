package job

import (
	"context"
	"maps"
	"time"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/daemon/hooks"
)

const (
	envJobName = "ZREPL_JOB_NAME"
	envJobErr  = "ZREPL_JOB_ERR"

	envJobSnapshots  = "ZREPL_LOCAL_SNAPSHOTS"
	envJobReplicated = "ZREPL_LOCAL_REPLICATED"
)

func NewHookFromConfig(in *config.HookCommand) *Hook {
	self := &Hook{
		path: in.Path,
		args: in.Args,
		env:  in.Env,

		errIsFatal: in.ErrIsFatal,
	}

	if in.Timeout != nil {
		self.timeout = *in.Timeout
	}
	return self
}

type Hook struct {
	path string
	args []string
	env  map[string]string

	timeout    time.Duration
	errIsFatal bool
}

func (self *Hook) ErrIsFatal() bool { return self.errIsFatal }

func (self *Hook) Run(ctx context.Context, j Job) error {
	return self.run(ctx, j, nil)
}

func (self *Hook) run(ctx context.Context, j Job, env map[string]string) error {
	cmd := hooks.NewCommand(self.path, self.args...).
		WithEnv(self.env, self.makeJobEnv(j, env)).
		WithTimeout(self.timeout)
	return cmd.Run(ctx)
}

func (self *Hook) makeJobEnv(j Job, runtime map[string]string,
) map[string]string {
	env := make(map[string]string, 4+len(runtime))
	env[envJobName] = j.Name()

	if jobStatus := j.Status(); jobStatus != nil {
		env[envJobErr] = jobStatus.Error()
		env[envJobSnapshots] = jobStatus.Snapshots()
		env[envJobReplicated] = jobStatus.Replicated()
	}

	maps.Copy(env, runtime)
	return env
}

func (self *Hook) RunEnv(ctx context.Context, j Job, env map[string]string,
) error {
	return self.run(ctx, j, env)
}
