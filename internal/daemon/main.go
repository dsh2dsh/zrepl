package daemon

import (
	"context"

	"github.com/dsh2dsh/zrepl/internal/cli"
)

var DaemonCmd = &cli.Subcommand{
	Use:   "daemon",
	Short: "run the zrepl daemon",

	ConfigWithIncludes: true,

	Run: func(ctx context.Context, subcommand *cli.Subcommand, args []string) error {
		return Run(ctx, subcommand.Config())
	},
}
