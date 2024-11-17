package pruner

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/pruning"
	"github.com/dsh2dsh/zrepl/internal/util/envconst"
)

type PrunerFactory struct {
	concurrency                    int
	senderRules                    []pruning.KeepRule
	receiverRules                  []pruning.KeepRule
	retryWait                      time.Duration
	considerSnapAtCursorReplicated bool
	promPruneSecs                  *prometheus.HistogramVec
}

type LocalPrunerFactory struct {
	concurrency   int
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

	concurrency := int(in.Concurrency)
	if concurrency == 0 {
		concurrency = runtime.GOMAXPROCS(0)
	}

	f := &LocalPrunerFactory{
		concurrency:   concurrency,
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

	var considerSnapAtCursorReplicated bool
	for _, r := range in.KeepSender {
		if knr, ok := r.Ret.(*config.PruneKeepNotReplicated); ok {
			considerSnapAtCursorReplicated = considerSnapAtCursorReplicated ||
				!knr.KeepSnapshotAtCursor
		}
	}

	concurrency := int(in.Concurrency)
	if concurrency == 0 {
		concurrency = runtime.GOMAXPROCS(0)
	}

	f := &PrunerFactory{
		concurrency:   concurrency,
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

func (f *PrunerFactory) Concurrency() int { return f.concurrency }

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

func (f *LocalPrunerFactory) Concurrency() int { return f.concurrency }
