package hooks

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"time"
)

func NewCommand(name string, arg ...string) *Cmd {
	return &Cmd{name: name, args: arg}
}

type Cmd struct {
	name string
	args []string
	env  []string

	timeout time.Duration

	cmd    *exec.Cmd
	output []byte
}

func (self *Cmd) WithEnv(envs ...map[string]string) *Cmd {
	var size int
	for _, env := range envs {
		size += len(env)
	}
	self.env = make([]string, 0, len(os.Environ())+size)
	self.env = append(self.env, os.Environ()...)

	for _, env := range envs {
		for k, v := range env {
			self.env = append(self.env, k+"="+v)
		}
	}
	return self
}

func (self *Cmd) WithTimeout(t time.Duration) *Cmd {
	self.timeout = t
	return self
}

func (self *Cmd) Run(ctx context.Context) error {
	if self.timeout > 0 {
		ctx2, cancel := context.WithTimeout(ctx, self.timeout)
		defer cancel()
		ctx = ctx2
	}

	cmd := exec.CommandContext(ctx, self.name, self.args...)
	cmd.Env = self.env
	self.cmd = cmd

	l := getLogger(ctx)
	l.Info("\"" + cmd.String() + "\"")

	output, err := cmd.Output()
	self.output = output
	logOutput(l, slog.LevelInfo, "stdout", self.output)

	if err != nil {
		return fmt.Errorf("exec %q: %w",
			self.String(), self.wrapError(ctx, l, err))
	}
	return nil
}

func (self *Cmd) wrapError(ctx context.Context, l *slog.Logger, err error,
) error {
	if err == nil {
		return nil
	}

	var ee *exec.ExitError
	if errors.As(err, &ee) && len(ee.Stderr) != 0 {
		logOutput(l, slog.LevelError, "stderr", ee.Stderr)
		self.output = slices.Concat(self.output, ee.Stderr)
	}

	if errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
		return fmt.Errorf("timed out after %s: %w", self.timeout, err)
	}
	return fmt.Errorf("exited with error: %w", err)
}

func (self *Cmd) String() string {
	if self.cmd != nil {
		return self.cmd.String()
	}
	return self.name
}

func (self *Cmd) CombinedOutput() []byte { return self.output }
