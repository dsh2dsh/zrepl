package client

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/dsh2dsh/zrepl/internal/cli"
	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/daemon"
)

var SignalCmd = &cli.Subcommand{
	Use:   "signal {stop | shutdown | wakeup JOB | reset JOB}",
	Short: "send a signal to the daemon",
	Long: `Send a signal to the daemon.

Expected signals:
  stop     Stop the daemon right now
  shutdown Stop the daemon gracefully
  wakeup   Wake up a job from wait state
  reset    Abort jobs current invocation
`,

	SetupCobra: func(cmd *cobra.Command) {
		cmd.ValidArgs = []string{"stop", "shutdown", "wakeup", "reset"}
		cmd.Args = cobra.MatchAll(cobra.MinimumNArgs(1),
			func(cmd *cobra.Command, args []string) error {
				switch args[0] {
				case "stop", "shutdown":
					return cobra.ExactArgs(1)(cmd, args)
				case "wakeup", "reset":
					return cobra.ExactArgs(2)(cmd, args)
				}
				return cobra.OnlyValidArgs(cmd, args)
			})
	},

	Run: func(ctx context.Context, subcommand *cli.Subcommand,
		args []string,
	) error {
		return runSignalCmd(subcommand.Config(), args)
	},
}

func runSignalCmd(config *config.Config, args []string) error {
	req := struct {
		Op   string
		Name string
	}{Op: args[0]}

	if len(args) > 1 {
		req.Name = args[1]
	}

	return jsonRequestResponse(config.Global.Control.SockPath,
		daemon.ControlJobEndpointSignal, &req, nil)
}
