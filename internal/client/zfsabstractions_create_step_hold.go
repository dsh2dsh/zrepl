package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/pflag"

	"github.com/dsh2dsh/zrepl/internal/cli"
	"github.com/dsh2dsh/zrepl/internal/endpoint"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

var zabsCreateStepHoldFlags struct {
	target string
	jobid  JobIDFlag
}

var zabsCmdCreateStepHold = &cli.Subcommand{
	Use:             "step",
	Run:             doZabsCreateStep,
	NoRequireConfig: true,
	Short:           `create a step hold or bookmark`,
	SetupFlags: func(f *pflag.FlagSet) {
		f.StringVarP(&zabsCreateStepHoldFlags.target, "target", "t", "", "snapshot to be held / bookmark to be held")
		f.VarP(&zabsCreateStepHoldFlags.jobid, "jobid", "j", "jobid for which the hold is installed")
	},
}

func doZabsCreateStep(ctx context.Context, sc *cli.Subcommand, args []string) error {
	if len(args) > 0 {
		return errors.New("subcommand takes no arguments")
	}

	f := &zabsCreateStepHoldFlags

	fs, _, _, err := zfs.DecomposeVersionString(f.target)
	if err != nil {
		return fmt.Errorf("%q invalid target: %w", f.target, err)
	}

	if f.jobid.FlagValue() == nil {
		return errors.New("jobid must be set")
	}

	v, err := zfs.ZFSGetFilesystemVersion(ctx, f.target)
	if err != nil {
		return fmt.Errorf("get info about target %q: %w", f.target, err)
	}

	step, err := endpoint.HoldStep(ctx, fs, v, *f.jobid.FlagValue())
	if err != nil {
		return fmt.Errorf("create step hold: %w", err)
	}
	fmt.Println(step.String())
	return nil
}
