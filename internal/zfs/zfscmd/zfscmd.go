// Package zfscmd provides a wrapper around packate os/exec.
// Functionality provided by the wrapper:
// - logging start and end of command execution
// - status report of active commands
// - prometheus metrics of runtimes
package zfscmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

func CommandContext(ctx context.Context, name string, args ...string) *Cmd {
	return New(ctx).WithCommand(name, args)
}

func New(ctx context.Context) *Cmd {
	return &Cmd{ctx: ctx, logError: true}
}

type Cmd struct {
	cmd                                      *exec.Cmd
	cmds                                     []*exec.Cmd
	ctx                                      context.Context
	mtx                                      sync.RWMutex
	startedAt, waitStartedAt, waitReturnedAt time.Time

	usage        usage
	stderrOutput []byte
	logError     bool

	cmdLogger Logger
}

func (c *Cmd) WithCommand(name string, args []string) *Cmd {
	c.cmd = exec.CommandContext(c.ctx, name, args...)
	c.cmds = append(c.cmds, c.cmd)
	return c
}

func (c *Cmd) WithLogError(v bool) *Cmd {
	c.logError = v
	return c
}

func (c *Cmd) WithPipeLen(n int) *Cmd {
	c.cmds = make([]*exec.Cmd, 0, n+1)
	return c
}

func (c *Cmd) WithStderrOutput(b []byte) *Cmd {
	c.stderrOutput = b
	return c
}

// err.(*exec.ExitError).Stderr will NOT be set
func (c *Cmd) CombinedOutput() (o []byte, err error) {
	c.startPre()
	c.startPost(nil)
	c.waitPre()
	o, err = c.cmd.CombinedOutput()
	c.stderrOutput = o
	c.waitPost(err)
	return
}

// err.(*exec.ExitError).Stderr will be set
func (c *Cmd) Output() (o []byte, err error) {
	c.startPre()
	c.startPost(nil)
	c.waitPre()
	o, err = c.cmd.Output()
	c.waitPost(err)
	return
}

func (c *Cmd) StdoutPipeWithErrorBuf(w io.Writer) (io.ReadCloser, error) {
	c.cmd.Stderr = w
	return c.cmd.StdoutPipe()
}

type Stdio struct {
	Stdin  io.ReadCloser
	Stdout io.Writer
	Stderr io.Writer
}

func (c *Cmd) SetStdio(stdio Stdio) {
	c.cmd.Stdin = stdio.Stdin
	c.cmd.Stderr = stdio.Stderr
	c.cmd.Stdout = stdio.Stdout
}

func (c *Cmd) String() string {
	var s strings.Builder
	for i, cmd := range c.cmds {
		s.WriteString(strings.Join(cmd.Args, " "))
		if i+1 < len(c.cmds) {
			s.WriteString(" | ")
		}
	}
	return s.String()
}

func (c *Cmd) Log() Logger { return c.logWithCmd() }

func (c *Cmd) logWithCmd() Logger {
	if c.cmdLogger == nil {
		c.cmdLogger = c.log().WithField("cmd", c.String())
	}
	return c.cmdLogger
}

func (c *Cmd) log() Logger {
	return getLogger(c.ctx)
}

// Start the command.
//
// If this method returns an error, the Cmd instance is invalid. Start must not
// be called repeatedly.
func (c *Cmd) Start() error {
	c.startPre()
	err := c.StartPipe()
	if err != nil {
		_ = c.WaitPipe()
	}
	c.startPost(err)
	return err
}

// Get the underlying os.Process.
//
// Only call this method after a successful call to .Start().
func (c *Cmd) Process() *os.Process {
	if c.startedAt.IsZero() {
		panic("calling Process() only allowed after successful call to Start()")
	}
	return c.cmd.Process
}

// Blocking wait for the process to exit.
// May be called concurrently and repeatly (exec.Cmd.Wait() semantics apply).
//
// Only call this method after a successful call to .Start().
func (c *Cmd) Wait() (err error) {
	c.waitPre()
	err = c.WaitPipe()
	c.waitPost(err)
	return err
}

func (c *Cmd) startPre() {
	startPreLogging(c, time.Now())
}

func (c *Cmd) startPost(err error) {
	now := time.Now()
	c.startedAt = now

	startPostReport(c, err)
	startPostLogging(c, err, now)
}

func (c *Cmd) waitPre() {
	now := time.Now()

	// ignore duplicate waits
	c.mtx.Lock()
	// ignore duplicate waits
	if !c.waitStartedAt.IsZero() {
		c.mtx.Unlock()
		return
	}
	c.waitStartedAt = now
	c.mtx.Unlock()

	waitPreLogging(c, now)
}

type usage struct {
	total_secs, system_secs, user_secs float64
}

func (c *Cmd) waitPost(err error) {
	now := time.Now()

	c.mtx.Lock()
	// ignore duplicate waits
	if !c.waitReturnedAt.IsZero() {
		c.mtx.Unlock()
		return
	}
	c.waitReturnedAt = now
	c.mtx.Unlock()

	// build usage
	var s *os.ProcessState
	if err == nil {
		s = c.cmd.ProcessState
	} else {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			s = exitError.ProcessState
			if c.stderrOutput == nil {
				c.stderrOutput = exitError.Stderr
			}
		}
	}

	if s == nil {
		c.usage = usage{
			total_secs:  c.Runtime().Seconds(),
			system_secs: -1,
			user_secs:   -1,
		}
	} else {
		c.usage = usage{
			total_secs:  c.Runtime().Seconds(),
			system_secs: s.SystemTime().Seconds(),
			user_secs:   s.UserTime().Seconds(),
		}
	}

	waitPostReport(c)
	if err == nil || c.logError {
		c.LogError(err, false)
	}
	waitPostPrometheus(c, c.usage)
}

func (c *Cmd) LogError(err error, debug bool) {
	waitPostLogging(c, err, debug)
}

// returns 0 if the command did not yet finish
func (c *Cmd) Runtime() time.Duration {
	if c.waitReturnedAt.IsZero() {
		return 0
	}
	return c.waitReturnedAt.Sub(c.startedAt)
}

func (c *Cmd) TestOnly_ExecCmd() *exec.Cmd {
	return c.cmd
}

func (c *Cmd) PipeTo(cmds [][]string, stdout io.ReadCloser, stderr io.Writer,
) (io.ReadCloser, error) {
	c.cmds = append(c.cmds, c.buildPipe(cmds)...)
	for _, cmd := range c.cmds {
		r, err := cmd.StdoutPipe()
		if err != nil {
			return nil, fmt.Errorf(
				"create stdout pipe from %q: %w", cmd.String(), err)
		}
		cmd.Stderr = stderr
		cmd.Stdin = stdout
		stdout = r
	}
	return stdout, nil
}

func (c *Cmd) buildPipe(cmds [][]string) []*exec.Cmd {
	pipeCmds := make([]*exec.Cmd, 0, len(cmds))
	for _, cmd := range cmds {
		name := cmd[0]
		var args []string
		if len(cmd) > 1 {
			args = cmd[1:]
		}
		pipeCmds = append(pipeCmds, exec.CommandContext(c.ctx, name, args...))
	}
	return pipeCmds
}

func (c *Cmd) PipeFrom(cmds [][]string, stdin io.ReadCloser, stdout,
	stderr io.Writer,
) error {
	c.cmds = c.cmds[:0]
	r, err := c.PipeTo(cmds, stdin, stderr)
	if err != nil {
		return err
	}

	c.cmds = append(c.cmds, c.cmd)
	c.SetStdio(Stdio{
		Stdin:  r,
		Stdout: stdout,
		Stderr: stderr,
	})
	return nil
}

func (c *Cmd) StartPipe() error {
	for _, cmd := range c.cmds {
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("start %q: %w", cmd.String(), err)
		}
	}
	return nil
}

func (c *Cmd) WaitPipe() error {
	var pipeErr error
	for i, cmd := range c.cmds {
		if cmd.Process == nil {
			break
		}
		err := cmd.Wait()
		if err != nil && pipeErr == nil && !errors.Is(err, os.ErrClosed) {
			pipeErr = fmt.Errorf("wait[%d] %q: %w", i, cmd.String(), err)
		}
	}

	if pipeErr != nil {
		if errors.Is(context.Cause(c.ctx), context.DeadlineExceeded) {
			return fmt.Errorf("timed out: %w", pipeErr)
		}
		return pipeErr
	}
	return nil
}

func (c *Cmd) WrapStdin(wrapper func(r io.Reader) io.Reader) *Cmd {
	if wrapper != nil {
		c.cmd.Stdin = wrapper(c.cmd.Stdin)
	}
	return c
}
