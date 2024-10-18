// See cmd package.
package main

import (
	"github.com/dsh2dsh/zrepl/internal/cli"
	"github.com/dsh2dsh/zrepl/internal/client"
	"github.com/dsh2dsh/zrepl/internal/client/monitor"
	"github.com/dsh2dsh/zrepl/internal/client/status"
	"github.com/dsh2dsh/zrepl/internal/daemon"
)

func init() {
	cli.AddSubcommand(daemon.DaemonCmd)
	cli.AddSubcommand(status.Subcommand)
	cli.AddSubcommand(client.SignalCmd)
	cli.AddSubcommand(client.StdinserverCmd)
	cli.AddSubcommand(client.ConfigcheckCmd)
	cli.AddSubcommand(client.VersionCmd)
	cli.AddSubcommand(client.TestCmd)
	cli.AddSubcommand(client.MigrateCmd)
	cli.AddSubcommand(client.ZFSAbstractionsCmd)
	cli.AddSubcommand(monitor.Subcommand)
}

func main() {
	cli.Run()
}
