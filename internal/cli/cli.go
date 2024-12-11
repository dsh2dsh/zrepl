package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/version"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

var rootArgs struct {
	configPath string
}

var rootCmd = &cobra.Command{
	Use:   "zrepl",
	Short: "One-stop ZFS replication solution",

	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// Don't show usage on app errors.
		// https://github.com/spf13/cobra/issues/340#issuecomment-378726225
		cmd.SilenceUsage = true
	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(
		&rootArgs.configPath, "config", "", "config file path")
}

var genCompletionCmd = &cobra.Command{
	Use:   "gencompletion",
	Short: "generate shell auto-completions",
}

type completionCmdInfo struct {
	genFunc func(outpath string) error
	help    string
}

var completionCmdMap = map[string]completionCmdInfo{
	"zsh": {
		rootCmd.GenZshCompletionFile,
		"  save to file `_zrepl` in your zsh's $fpath",
	},
	"bash": {
		rootCmd.GenBashCompletionFile,
		"  save to a path and source that path in your .bashrc",
	},
}

func init() {
	for sh, info := range completionCmdMap {
		genCompletionCmd.AddCommand(&cobra.Command{
			Use:     sh + " path/to/out/file",
			Short:   fmt.Sprintf("generate %s completions", sh),
			Example: info.help,
			Run: func(cmd *cobra.Command, args []string) {
				if len(args) != 1 {
					fmt.Fprintf(os.Stderr, "specify exactly one positional agument\n")
					err := cmd.Usage()
					if err != nil {
						panic(err)
					}
					os.Exit(1)
				}
				if err := info.genFunc(args[0]); err != nil {
					fmt.Fprintf(os.Stderr, "error generating %s completion: %s", sh, err)
					os.Exit(1)
				}
			},
		})
	}
	rootCmd.AddCommand(genCompletionCmd)
}

type Subcommand struct {
	Use     string
	Short   string
	Long    string
	Example string

	NoRequireConfig    bool
	ConfigWithIncludes bool

	Run func(ctx context.Context, subcommand *Subcommand,
		args []string) error
	SetupFlags       func(f *pflag.FlagSet)
	SetupSubcommands func() []*Subcommand
	SetupCobra       func(c *cobra.Command)

	config    *config.Config
	configErr error
}

func (s *Subcommand) ConfigParsingError() error {
	return s.configErr
}

func (s *Subcommand) Config() *config.Config {
	if !s.NoRequireConfig && s.config == nil {
		panic("command that requires config is running and has no config set")
	}
	return s.config
}

func (s *Subcommand) run(cmd *cobra.Command, args []string) {
	s.tryParseConfig()
	ctx := context.Background()
	err := s.Run(ctx, s, args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func (s *Subcommand) tryParseConfig() {
	opts := make([]config.Option, 0, 1)
	if !s.ConfigWithIncludes {
		opts = append(opts, config.WithoutIncludes())
	}

	config, err := config.ParseConfig(rootArgs.configPath, opts...)
	s.configErr = err
	if err != nil {
		if s.NoRequireConfig {
			// doesn't matter
			return
		} else {
			fmt.Fprintf(os.Stderr, "could not parse config: %s\n", err)
			os.Exit(1)
		}
	}
	s.config = config
	zfs.ZfsBin = config.Global.ZfsBin
}

func AddSubcommand(s *Subcommand) {
	addSubcommandToCobraCmd(rootCmd, s)
}

func addSubcommandToCobraCmd(c *cobra.Command, s *Subcommand) {
	cmd := cobra.Command{
		Use:     s.Use,
		Short:   s.Short,
		Long:    s.Long,
		Example: s.Example,
	}
	if s.Run != nil {
		cmd.Run = s.run
	}
	if s.SetupSubcommands != nil {
		for _, sub := range s.SetupSubcommands() {
			addSubcommandToCobraCmd(&cmd, sub)
		}
	}
	if s.SetupFlags != nil {
		s.SetupFlags(cmd.Flags())
	}
	if s.SetupCobra != nil {
		s.SetupCobra(&cmd)
	}
	c.AddCommand(&cmd)
}

func Run() {
	rootCmd.Version = version.NewZreplVersionInformation().Version
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
