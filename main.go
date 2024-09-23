// See cmd package.
package main

import (
	"github.com/dsh2dsh/zrepl/cli"
	"github.com/dsh2dsh/zrepl/client"
	"github.com/dsh2dsh/zrepl/client/monitor"
	"github.com/dsh2dsh/zrepl/client/status"
	"github.com/dsh2dsh/zrepl/daemon"
)

func init() {
	cli.AddSubcommand(daemon.DaemonCmd)
	cli.AddSubcommand(status.Subcommand)
	cli.AddSubcommand(client.SignalCmd)
	cli.AddSubcommand(client.StdinserverCmd)
	cli.AddSubcommand(client.ConfigcheckCmd)
	cli.AddSubcommand(client.VersionCmd)
	cli.AddSubcommand(client.PprofCmd)
	cli.AddSubcommand(client.TestCmd)
	cli.AddSubcommand(client.MigrateCmd)
	cli.AddSubcommand(client.ZFSAbstractionsCmd)
	cli.AddSubcommand(monitor.Subcommand)
}

func main() {
	cli.Run()
}
