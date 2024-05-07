package client

import (
	"context"
	"errors"

	"github.com/dsh2dsh/zrepl/cli"
	"github.com/dsh2dsh/zrepl/config"
	"github.com/dsh2dsh/zrepl/daemon"
)

var SignalCmd = &cli.Subcommand{
	Use:   "signal [wakeup|reset] JOB",
	Short: "wake up a job from wait state or abort its current invocation",
	Run: func(ctx context.Context, subcommand *cli.Subcommand, args []string) error {
		return runSignalCmd(subcommand.Config(), args)
	},
}

func runSignalCmd(config *config.Config, args []string) error {
	if len(args) != 2 {
		return errors.New("Expected 2 arguments: [wakeup|reset] JOB")
	}

	httpc, err := controlHttpClient(config.Global.Control.SockPath)
	if err != nil {
		return err
	}

	err = jsonRequestResponse(httpc, daemon.ControlJobEndpointSignal,
		struct {
			Name string
			Op   string
		}{
			Name: args[1],
			Op:   args[0],
		},
		struct{}{},
	)
	return err
}
