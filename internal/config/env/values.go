package env

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

var Values = struct {
	CreatetxgRangeBoundAllow bool `env:"ZREPL_ENDPOINT_LIST_ABSTRACTIONS_QUERY_CREATETXG_RANGE_BOUND_ALLOW_0"`

	PrunerRetryInterval time.Duration `env:"ZREPL_PRUNER_RETRY_INTERVAL"`

	SnapperSyncUpWarnMin time.Duration `env:"ZREPL_SNAPPER_SYNCUP_WARN_MIN_DURATION"`

	ReplicationMaxAttempts int `env:"ZREPL_REPLICATION_MAX_ATTEMPTS"`

	ReplicationReconnectHardTimeout time.Duration `env:"ZREPL_REPLICATION_RECONNECT_HARD_FAIL_TIMEOUT"`

	ZFSMaxHoldTagLen int `env:"ZREPL_ZFS_MAX_HOLD_TAG_LEN"`
}{
	PrunerRetryInterval:             10 * time.Second,
	ReplicationMaxAttempts:          3,
	ReplicationReconnectHardTimeout: 10 * time.Minute,
	SnapperSyncUpWarnMin:            time.Second,

	// 256 include NULL byte, from module/zfs/dsl_userhold.c
	ZFSMaxHoldTagLen: 256 - 1,
}

func Parse() error {
	if err := env.Parse(&Values); err != nil {
		return fmt.Errorf("failed parse env vars: %w", err)
	}
	return nil
}
