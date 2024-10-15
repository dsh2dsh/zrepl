package logging

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"iter"
	"log/syslog"
	"os"

	"github.com/dsh2dsh/zrepl/config"
	"github.com/dsh2dsh/zrepl/daemon/logging/trace"
	"github.com/dsh2dsh/zrepl/logger"
	"github.com/dsh2dsh/zrepl/tlsconf"
)

func OutletsFromConfig(in config.LoggingOutletEnumList,
) (*logger.Outlets, error) {
	outlets := logger.NewOutlets()
	if len(in) == 0 {
		// Default config
		out := WriterOutlet{NewSlogFormatter(), os.Stdout}
		outlets.Add(out, logger.Warn)
		return outlets, nil
	}

	var syslogOutlets, stdoutOutlets int
	for lei, le := range in {
		outlet, minLevel, err := ParseOutlet(le)
		if err != nil {
			return nil, fmt.Errorf("cannot parse outlet #%d: %w", lei, err)
		}
		var _ logger.Outlet = WriterOutlet{}
		var _ logger.Outlet = &SyslogOutlet{}
		switch outlet.(type) {
		case *SyslogOutlet:
			syslogOutlets++
		case WriterOutlet:
			stdoutOutlets++
		}
		outlets.Add(outlet, minLevel)
	}

	if syslogOutlets > 1 {
		return nil, errors.New("can only define one 'syslog' outlet")
	}
	if stdoutOutlets > 1 {
		return nil, errors.New("can only define one 'stdout' outlet")
	}
	return outlets, nil
}

type Subsystem string

const (
	SubsysCron         Subsystem = "cron"
	SubsysMeta         Subsystem = "meta"
	SubsysJob          Subsystem = "job"
	SubsysReplication  Subsystem = "repl"
	SubsysEndpoint     Subsystem = "endpoint"
	SubsysPruning      Subsystem = "pruning"
	SubsysSnapshot     Subsystem = "snapshot"
	SubsysHooks        Subsystem = "hook"
	SubsysTransport    Subsystem = "transport"
	SubsysTransportMux Subsystem = "transportmux"
	SubsysRPC          Subsystem = "rpc"
	SubsysRPCControl   Subsystem = "rpc.ctrl"
	SubsysRPCData      Subsystem = "rpc.data"
	SubsysZFSCmd       Subsystem = "zfs.cmd"
	SubsysTraceData    Subsystem = "trace.data"
	SubsysPlatformtest Subsystem = "platformtest"
)

var AllSubsystems = []Subsystem{
	SubsysCron,
	SubsysMeta,
	SubsysJob,
	SubsysReplication,
	SubsysEndpoint,
	SubsysPruning,
	SubsysSnapshot,
	SubsysHooks,
	SubsysTransport,
	SubsysTransportMux,
	SubsysRPC,
	SubsysRPCControl,
	SubsysRPCData,
	SubsysZFSCmd,
	SubsysTraceData,
	SubsysPlatformtest,
}

func WithInjectedField(ctx context.Context, field string, value any,
) context.Context {
	parent, _ := ctx.Value(contextKeyInjectedField).(*injectedField)
	// TODO sanity-check `field` now
	return context.WithValue(ctx, contextKeyInjectedField,
		&injectedField{field, value, parent})
}

type injectedField struct {
	field  string
	value  any
	parent *injectedField
}

func iterInjectedFields(ctx context.Context) iter.Seq2[string, any] {
	inj, _ := ctx.Value(contextKeyInjectedField).(*injectedField)
	fn := func(yield func(field string, value any) bool) {
		for ; inj != nil; inj = inj.parent {
			if !yield(inj.field, inj.value) {
				return
			}
		}
	}
	return fn
}

func WithLogger(ctx context.Context, l logger.Logger) context.Context {
	return context.WithValue(ctx, contextKeyLoggers, l)
}

func GetLogger(ctx context.Context, subsys Subsystem) logger.Logger {
	l, _ := ctx.Value(contextKeyLoggers).(logger.Logger)
	if l == nil {
		return logger.NewNullLogger()
	}

	l = l.WithField(SubsysField, subsys).WithField(SpanField,
		trace.GetSpanStackOrDefault(ctx, *trace.StackKindId, "NOSPAN"))

	for field, v := range iterInjectedFields(ctx) {
		l = l.WithField(field, v)
	}
	return l
}

func parseLogFormat(common config.LoggingOutletCommon,
) (f EntryFormatter, err error) {
	switch common.Format {
	case "human":
		return parseSlogFormatter(&common).WithTextHandler(), nil
	case "logfmt":
		return parseSlogFormatter(&common).WithTextHandler(), nil
	case "json":
		return parseSlogFormatter(&common).WithJsonHandler(), nil
	case "text":
		return parseSlogFormatter(&common).WithTextHandler(), nil
	default:
		return nil, fmt.Errorf("invalid log format: '%s'", common.Format)
	}
}

func ParseOutlet(in config.LoggingOutletEnum,
) (o logger.Outlet, level logger.Level, err error) {
	parseCommon := func(common config.LoggingOutletCommon,
	) (logger.Level, EntryFormatter, error) {
		if common.Level == "" || common.Format == "" {
			return 0, nil, errors.New("must specify 'level' and 'format' field")
		}
		minLevel, err := logger.ParseLevel(common.Level)
		if err != nil {
			return 0, nil, fmt.Errorf("cannot parse 'level' field: %w", err)
		}
		formatter, err := parseLogFormat(common)
		if err != nil {
			return 0, nil, fmt.Errorf("cannot parse 'formatter' field: %w", err)
		}
		return minLevel, formatter, nil
	}

	var f EntryFormatter

	switch v := in.Ret.(type) {
	case *config.TCPLoggingOutlet:
		level, f, err = parseCommon(v.LoggingOutletCommon)
		if err != nil {
			break
		}
		o, err = parseTCPOutlet(v, f)
	case *config.SyslogLoggingOutlet:
		level, f, err = parseCommon(v.LoggingOutletCommon)
		if err != nil {
			break
		}
		o, err = parseSyslogOutlet(v, f)
	case *config.FileLoggingOutlet:
		level, f, err = parseCommon(v.LoggingOutletCommon)
		if err != nil {
			break
		}
		o, err = parseFileOutlet(v, f)
	default:
		panic(v)
	}
	return o, level, err
}

func parseTCPOutlet(in *config.TCPLoggingOutlet, formatter EntryFormatter) (out *TCPOutlet, err error) {
	var tlsConfig *tls.Config
	if in.TLS != nil {
		tlsConfig, err = func(m *config.TCPLoggingOutletTLS, host string) (*tls.Config, error) {
			clientCert, err := tls.LoadX509KeyPair(m.Cert, m.Key)
			if err != nil {
				return nil, fmt.Errorf("cannot load client cert: %w", err)
			}

			var rootCAs *x509.CertPool
			if m.CA == "" {
				if rootCAs, err = x509.SystemCertPool(); err != nil {
					return nil, fmt.Errorf("cannot open system cert pool: %w", err)
				}
			} else {
				rootCAs, err = tlsconf.ParseCAFile(m.CA)
				if err != nil {
					return nil, fmt.Errorf("cannot parse CA cert: %w", err)
				}
			}
			if rootCAs == nil {
				panic("invariant violated")
			}

			return tlsconf.ClientAuthClient(host, rootCAs, clientCert)
		}(in.TLS, in.Address)
		if err != nil {
			return nil, errors.New("cannot not parse TLS config in field 'tls'")
		}
	}

	formatter.SetMetadataFlags(MetadataAll)
	return NewTCPOutlet(formatter, in.Net, in.Address, tlsConfig, in.RetryInterval), nil
}

func parseSyslogOutlet(in *config.SyslogLoggingOutlet, formatter EntryFormatter) (out *SyslogOutlet, err error) {
	out = &SyslogOutlet{}
	out.Formatter = formatter
	out.Formatter.SetMetadataFlags(MetadataNone)
	out.Facility = syslog.Priority(in.Facility)
	out.RetryInterval = in.RetryInterval
	return out, nil
}
