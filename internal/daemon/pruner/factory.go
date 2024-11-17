package pruner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/pruning"
	"github.com/dsh2dsh/zrepl/internal/util/envconst"
)

type PrunerFactory struct {
	senderRules                    []pruning.KeepRule
	receiverRules                  []pruning.KeepRule
	retryWait                      time.Duration
	considerSnapAtCursorReplicated bool
	promPruneSecs                  *prometheus.HistogramVec
}

type LocalPrunerFactory struct {
	keepRules     []pruning.KeepRule
	retryWait     time.Duration
	promPruneSecs *prometheus.HistogramVec
}

func NewLocalPrunerFactory(in config.PruningLocal,
	promPruneSecs *prometheus.HistogramVec,
) (*LocalPrunerFactory, error) {
	rules, err := pruning.RulesFromConfig(in.Keep)
	if err != nil {
		return nil, fmt.Errorf("cannot build pruning rules: %w", err)
	}
	for _, r := range in.Keep {
		if _, ok := r.Ret.(*config.PruneKeepNotReplicated); ok {
			// rule NotReplicated  for a local pruner doesn't make sense
			// because no replication happens with that job type
			return nil, errors.New(
				"single-site pruner cannot support `not_replicated` keep rule")
		}
	}
	f := &LocalPrunerFactory{
		keepRules:     rules,
		promPruneSecs: promPruneSecs,

		retryWait: envconst.Duration(
			"ZREPL_PRUNER_RETRY_INTERVAL", 10*time.Second),
	}
	return f, nil
}

func NewPrunerFactory(in config.PruningSenderReceiver,
	promPruneSecs *prometheus.HistogramVec,
) (*PrunerFactory, error) {
	keepRulesReceiver, err := pruning.RulesFromConfig(in.KeepReceiver)
	if err != nil {
		return nil, fmt.Errorf("cannot build receiver pruning rules: %w", err)
	}

	keepRulesSender, err := pruning.RulesFromConfig(in.KeepSender)
	if err != nil {
		return nil, fmt.Errorf("cannot build sender pruning rules: %w", err)
	}

	considerSnapAtCursorReplicated := false
	for _, r := range in.KeepSender {
		knr, ok := r.Ret.(*config.PruneKeepNotReplicated)
		if !ok {
			continue
		}
		considerSnapAtCursorReplicated = considerSnapAtCursorReplicated ||
			!knr.KeepSnapshotAtCursor
	}
	f := &PrunerFactory{
		senderRules:   keepRulesSender,
		receiverRules: keepRulesReceiver,
		promPruneSecs: promPruneSecs,

		retryWait: envconst.Duration(
			"ZREPL_PRUNER_RETRY_INTERVAL", 10*time.Second),

		considerSnapAtCursorReplicated: considerSnapAtCursorReplicated,
	}
	return f, nil
}

func (f *PrunerFactory) BuildSenderPruner(ctx context.Context, target Target,
	sender Sender,
) *Pruner {
	return &Pruner{
		args: args{
			context.WithValue(ctx, contextKeyPruneSide, "sender"),
			target,
			sender,
			f.senderRules,
			f.retryWait,
			f.considerSnapAtCursorReplicated,
			f.promPruneSecs.WithLabelValues("sender"),
		},
		state:     Plan,
		startedAt: time.Now(),
	}
}

func (f *PrunerFactory) BuildReceiverPruner(ctx context.Context, target Target,
	sender Sender,
) *Pruner {
	return &Pruner{
		args: args{
			context.WithValue(ctx, contextKeyPruneSide, "receiver"),
			target,
			sender,
			f.receiverRules,
			f.retryWait,
			false, // senseless here anyways
			f.promPruneSecs.WithLabelValues("receiver"),
		},
		state:     Plan,
		startedAt: time.Now(),
	}
}

func (f *LocalPrunerFactory) BuildLocalPruner(ctx context.Context,
	target Target, history Sender,
) *Pruner {
	return &Pruner{
		args: args{
			context.WithValue(ctx, contextKeyPruneSide, "local"),
			target,
			history,
			f.keepRules,
			f.retryWait,
			// considerSnapAtCursorReplicated is not relevant for local pruning
			false,
			f.promPruneSecs.WithLabelValues("local"),
		},
		state:     Plan,
		startedAt: time.Now(),
	}
}
