package hooks

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

type CommandHookReport struct {
	Command string
	Args    []string // currently always empty
	Env     map[string]string
	Err     error

	CombinedOutput []byte
}

func (r *CommandHookReport) String() string {
	// Reproduces a POSIX shell-compatible command line
	var cmdLine strings.Builder
	sep := ""

	// Make sure environment variables are always
	// printed in the same order
	envKeys := slices.Sorted(maps.Keys(r.Env))
	for _, k := range envKeys {
		cmdLine.WriteString(fmt.Sprintf("%s%s='%s'", sep, k, r.Env[k]))
		sep = " "
	}

	cmdLine.WriteString(sep)
	cmdLine.WriteString(r.Command)
	for _, a := range r.Args {
		cmdLine.WriteString(fmt.Sprintf("%s'%s'", sep, a))
	}

	var msg string
	if r.Err == nil {
		msg = "command hook"
	} else {
		msg = fmt.Sprintf("command hook failed with %q", r.Err)
	}
	// no %q to make copy-pastable
	return fmt.Sprintf("%s: \"%s\"", msg, cmdLine.String())
}

func (r *CommandHookReport) Error() string {
	if r.Err == nil {
		return ""
	}
	return fmt.Sprintf("%s FAILED with error: %s", r.String(), r.Err)
}

func (r *CommandHookReport) HadError() bool { return r.Err != nil }
