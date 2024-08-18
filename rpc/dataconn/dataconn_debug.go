package dataconn

import (
	"fmt"
	"os"
)

//nolint:unused // keep it for debugging
var debugEnabled bool = false

func init() {
	if os.Getenv("ZREPL_RPC_DATACONN_DEBUG") != "" {
		debugEnabled = true
	}
}

//nolint:unused // keep it for debugging
func debug(format string, args ...interface{}) {
	if debugEnabled {
		fmt.Fprintf(os.Stderr, "rpc/dataconn: %s\n", fmt.Sprintf(format, args...))
	}
}
