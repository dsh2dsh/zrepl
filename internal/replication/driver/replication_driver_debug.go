package driver

import (
	"fmt"
	"os"
)

var debugEnabled bool = false

func init() {
	if os.Getenv("ZREPL_REPLICATION_DRIVER_DEBUG") != "" {
		debugEnabled = true
	}
}

func debug(format string, args ...any) {
	if debugEnabled {
		fmt.Fprintf(os.Stderr, "repl: driver: %s\n", fmt.Sprintf(format, args...))
	}
}

type debugFunc func(format string, args ...any)

func debugPrefix(prefixFormat string, prefixFormatArgs ...any) debugFunc {
	prefix := fmt.Sprintf(prefixFormat, prefixFormatArgs...)
	return func(format string, args ...any) {
		debug("%s: %s", prefix, fmt.Sprintf(format, args...))
	}
}
