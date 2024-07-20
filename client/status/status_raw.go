package status

import (
	"bytes"
	"context"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/dsh2dsh/zrepl/cli"
)

var rawCmd = &cli.Subcommand{
	Use:   "raw",
	Short: "output daemon status information as JSON",

	SetupCobra: func(cmd *cobra.Command) {
		cmd.Args = cobra.ExactArgs(0)
	},

	Run: func(ctx context.Context, subcommand *cli.Subcommand, args []string,
	) error {
		return withStatusClient(subcommand, func(c *Client) error {
			return raw(c)
		})
	},
}

func raw(c *Client) error {
	b, err := c.StatusRaw()
	if err != nil {
		return err
	}
	if _, err := io.Copy(os.Stdout, bytes.NewReader(b)); err != nil {
		return err
	}
	return nil
}
