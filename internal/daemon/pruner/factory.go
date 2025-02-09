package pruner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/config/env"
	"github.com/dsh2dsh/zrepl/internal/pruning"
)

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
		concurrency:   int(in.Concurrency),
		keepRules:     rules,
		promPruneSecs: promPruneSecs,

		retryWait: env.Values.PrunerRetryInterval,
	}
	return f, nil
}

type LocalPrunerFactory struct {
	concurrency   int
	keepRules     []pruning.KeepRule
	retryWait     time.Duration
	promPruneSecs *prometheus.HistogramVec
}

func (f *LocalPrunerFactory) BuildLocalPruner(ctx context.Context,
	target Target, history Sender,
) *Pruner {
	return &Pruner{
		args: args{
			concurrency: f.concurrency,
			ctx:         context.WithValue(ctx, contextKeyPruneSide, "local"),
			target:      target,
			sender:      history,
			rules:       f.keepRules,
			retryWait:   f.retryWait,

			// considerSnapAtCursorReplicated is not relevant for local pruning
			considerSnapAtCursorReplicated: false,

			promPruneSecs: f.promPruneSecs.WithLabelValues("local"),
		},
		state:     Plan,
		startedAt: time.Now(),
	}
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

	var considerSnapAtCursorReplicated bool
	for _, r := range in.KeepSender {
		if knr, ok := r.Ret.(*config.PruneKeepNotReplicated); ok {
			considerSnapAtCursorReplicated = considerSnapAtCursorReplicated ||
				!knr.KeepSnapshotAtCursor
		}
	}

	f := &PrunerFactory{
		concurrency:   int(in.Concurrency),
		senderRules:   keepRulesSender,
		receiverRules: keepRulesReceiver,
		promPruneSecs: promPruneSecs,

		retryWait: env.Values.PrunerRetryInterval,

		considerSnapAtCursorReplicated: considerSnapAtCursorReplicated,
	}
	return f, nil
}

type PrunerFactory struct {
	concurrency                    int
	senderRules                    []pruning.KeepRule
	receiverRules                  []pruning.KeepRule
	retryWait                      time.Duration
	considerSnapAtCursorReplicated bool
	promPruneSecs                  *prometheus.HistogramVec
}

func (f *PrunerFactory) BuildSenderPruner(ctx context.Context, target Target,
	sender Sender,
) *Pruner {
	return &Pruner{
		args: args{
			concurrency: f.concurrency,
			ctx:         context.WithValue(ctx, contextKeyPruneSide, "sender"),
			target:      target,
			sender:      sender,
			rules:       f.senderRules,
			retryWait:   f.retryWait,

			considerSnapAtCursorReplicated: f.considerSnapAtCursorReplicated,

			promPruneSecs: f.promPruneSecs.WithLabelValues("sender"),
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
			concurrency: f.concurrency,
			ctx:         context.WithValue(ctx, contextKeyPruneSide, "receiver"),
			target:      target,
			sender:      sender,
			rules:       f.receiverRules,
			retryWait:   f.retryWait,

			considerSnapAtCursorReplicated: false, // senseless here anyways

			promPruneSecs: f.promPruneSecs.WithLabelValues("receiver"),
		},
		state:     Plan,
		startedAt: time.Now(),
	}
}
