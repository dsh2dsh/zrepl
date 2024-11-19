package logging

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"log/syslog"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/logger"
	"github.com/dsh2dsh/zrepl/internal/tlsconf"
)

const (
	SubsysCron        Subsystem = "cron"
	SubsysJob         Subsystem = "job"
	SubsysReplication Subsystem = "repl"
	SubsysEndpoint    Subsystem = "endpoint"
	SubsysPruning     Subsystem = "pruning"
	SubsysSnapshot    Subsystem = "snapshot"
	SubsysHooks       Subsystem = "hook"
	SubsysZFSCmd      Subsystem = "zfs.cmd"
)

type ctxKey struct{}

var ctxKeyLogger ctxKey = struct{}{}

type Subsystem string

func OutletsFromConfig(in config.LoggingOutletEnumList,
) (*logger.Outlets, error) {
	outlets := logger.NewOutlets()
	if len(in) == 0 {
		// Default config
		o, err := NewFileOutlet("", NewSlogFormatter())
		if err != nil {
			return nil, err
		}
		outlets.Add(o)
		return outlets, nil
	}

	for lei, le := range in {
		outlet, err := ParseOutlet(le)
		if err != nil {
			return nil, fmt.Errorf("cannot parse outlet #%d: %w", lei, err)
		}
		outlets.Add(outlet)
	}
	return outlets, nil
}

func WithField(ctx context.Context, field string, value any,
) context.Context {
	return WithLogger(ctx, FromContext(ctx).WithField(field, value))
}

func WithLogger(ctx context.Context, l *logger.Logger) context.Context {
	return context.WithValue(ctx, ctxKeyLogger, l)
}

func GetLogger(ctx context.Context, subsys Subsystem) *logger.Logger {
	return FromContext(ctx).WithField(SubsysField, subsys)
}

func FromContext(ctx context.Context) *logger.Logger {
	l, ok := ctx.Value(ctxKeyLogger).(*logger.Logger)
	if ok && l != nil {
		return l
	}
	return logger.NewNullLogger()
}

func parseLogFormat(common config.LoggingOutletCommon) (*SlogFormatter, error) {
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

func ParseOutlet(in config.LoggingOutletEnum) (o slog.Handler, err error) {
	parseCommon := func(common config.LoggingOutletCommon,
	) (*SlogFormatter, error) {
		if common.Level == "" || common.Format == "" {
			return nil, errors.New("must specify 'level' and 'format' field")
		}
		minLevel, err := parseLevel(common.Level)
		if err != nil {
			return nil, fmt.Errorf("cannot parse 'level' field: %w", err)
		}
		formatter, err := parseLogFormat(common)
		if err != nil {
			return nil, fmt.Errorf("cannot parse 'formatter' field: %w", err)
		}
		formatter.WithLevel(minLevel)
		return formatter, nil
	}

	var f *SlogFormatter
	switch v := in.Ret.(type) {
	case *config.TCPLoggingOutlet:
		f, err = parseCommon(v.LoggingOutletCommon)
		if err != nil {
			break
		}
		o, err = parseTCPOutlet(v, f)
	case *config.SyslogLoggingOutlet:
		f, err = parseCommon(v.LoggingOutletCommon)
		if err != nil {
			break
		}
		o, err = parseSyslogOutlet(v, f)
	case *config.FileLoggingOutlet:
		f, err = parseCommon(v.LoggingOutletCommon)
		if err != nil {
			break
		}
		o, err = parseFileOutlet(v, f)
	default:
		panic(v)
	}
	return o, err
}

func parseLevel(s string) (l slog.Level, err error) {
	if err = l.UnmarshalText([]byte(s)); err != nil {
		err = fmt.Errorf("unparseable level '%s': %w", s, err)
	}
	return
}

func parseTCPOutlet(in *config.TCPLoggingOutlet, formatter *SlogFormatter,
) (_ *TCPOutlet, err error) {
	var tlsConfig *tls.Config
	if in.TLS != nil {
		tlsConfig, err = func(m *config.TCPLoggingOutletTLS, host string,
		) (*tls.Config, error) {
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

	o := NewTCPOutlet(formatter.WithLogMetadata(true), in.Net, in.Address,
		tlsConfig, in.RetryInterval)
	return o, nil
}

func parseSyslogOutlet(in *config.SyslogLoggingOutlet, formatter *SlogFormatter,
) (*SyslogOutlet, error) {
	o := NewSyslogOutlet(formatter, syslog.Priority(in.Facility),
		in.RetryInterval)
	return o, nil
}
