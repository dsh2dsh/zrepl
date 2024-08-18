package rpc

import (
	"fmt"
	"os"
)

//nolint:unused // keep it for debugging
var debugEnabled bool = false

func init() {
	if os.Getenv("ZREPL_RPC_DEBUG") != "" {
		debugEnabled = true
	}
}

//nolint:unused // keep it for debugging
func debug(format string, args ...interface{}) {
	if debugEnabled {
		fmt.Fprintf(os.Stderr, "rpc: %s\n", fmt.Sprintf(format, args...))
	}
}
