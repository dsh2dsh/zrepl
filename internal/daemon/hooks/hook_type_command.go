package hooks

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"strings"
	"sync"
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
	extra Env,
) Env {
	r := Env{EnvTimeout: fmt.Sprintf("%.f", math.Floor(timeout.Seconds()))}

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

type Env map[string]string

func NewCommandHook(in *config.HookCommand) (*CommandHook, error) {
	r := &CommandHook{
		edge:       Pre | Post,
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
	edge       Edge
	filter     zfs.DatasetFilter
	errIsFatal bool
	command    string
	timeout    time.Duration
}

func (h *CommandHook) Filesystems() zfs.DatasetFilter { return h.filter }

func (h *CommandHook) ErrIsFatal() bool {
	return h.errIsFatal
}

func (h *CommandHook) String() string {
	return h.command
}

func (h *CommandHook) Run(ctx context.Context, edge Edge, phase Phase,
	dryRun bool, extra Env, state map[any]any,
) HookReport {
	l := getLogger(ctx).WithField("command", h.command)
	if h.timeout > 0 {
		cmdCtx, cancel := context.WithTimeout(ctx, h.timeout)
		defer cancel()
		ctx = cmdCtx
	}

	cmdExec := exec.CommandContext(ctx, h.command)
	hookEnv := NewHookEnv(edge, phase, dryRun, h.timeout, extra)
	cmdEnv := os.Environ()
	for k, v := range hookEnv {
		cmdEnv = append(cmdEnv, fmt.Sprintf("%s=%s", k, v))
	}
	cmdExec.Env = cmdEnv

	var scanMutex sync.Mutex
	logErrWriter := NewLogWriter(&scanMutex, l, logger.Warn, "stderr")
	logOutWriter := NewLogWriter(&scanMutex, l, logger.Info, "stdout")
	defer logErrWriter.Close()
	defer logOutWriter.Close()

	var combinedOutput bytes.Buffer
	cmdExec.Stderr = io.MultiWriter(logErrWriter, &combinedOutput)
	cmdExec.Stdout = io.MultiWriter(logOutWriter, &combinedOutput)

	report := &CommandHookReport{
		Command: h.command,
		Env:     hookEnv,
		// no report.Args
	}

	if err := cmdExec.Start(); err != nil {
		report.Err = err
		return report
	}

	if err := cmdExec.Wait(); err != nil {
		if errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
			report.Err = fmt.Errorf("timed out after %s: %s", h.timeout, err)
		} else {
			report.Err = err
		}
	}

	report.CombinedOutput = make([]byte, combinedOutput.Len())
	copy(report.CombinedOutput, combinedOutput.Bytes())
	return report
}
