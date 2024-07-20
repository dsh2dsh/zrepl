package status

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/dsh2dsh/zrepl/cli"
)

var (
	selectedJob     string
	refreshInterval time.Duration
)

var Subcommand = &cli.Subcommand{
	Use:   "status",
	Short: "display daemon status information",

	SetupCobra: func(cmd *cobra.Command) {
		cmd.Args = cobra.ExactArgs(0)
		addSelectedJob(cmd)
		cmd.Flags().DurationVarP(&refreshInterval, "delay", "d", 1*time.Second,
			"refresh interval")
	},

	SetupSubcommands: func() []*cli.Subcommand {
		return []*cli.Subcommand{dumpCmd, rawCmd}
	},

	Run: func(ctx context.Context, subcommand *cli.Subcommand, args []string,
	) error {
		return withStatusClient(subcommand, func(c *Client) error {
			return interactive(c, refreshInterval, selectedJob)
		})
	},
}

func addSelectedJob(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&selectedJob, "job", "j", "",
		"only show specified job")
}

func withStatusClient(subcommand *cli.Subcommand, fn func(c *Client) error,
) error {
	sockPath := subcommand.Config().Global.Control.SockPath
	statusClient, err := NewClient("unix", sockPath)
	if err != nil {
		return fmt.Errorf("connect to daemon socket at %q: %w", sockPath, err)
	}
	return fn(statusClient)
}
