package config

import (
	"fmt"
	"log/syslog"
	"testing"

	"github.com/creasty/defaults"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testValidGlobalSection(t *testing.T, s string) *Config {
	jobdef := `
jobs:
- name: dummyjob
  type: sink
  serve:
    type: tcp
    listen: ":2342"
    clients: {
      "10.0.0.1":"foo"
    }
  root_fs: zroot/foo
`
	_, err := ParseConfigBytes([]byte(jobdef))
	require.NoError(t, err)
	return testValidConfig(t, s+jobdef)
}

func TestOutletTypes(t *testing.T) {
	conf := testValidGlobalSection(t, `
global:
  logging:
  - type: stdout
    level: debug
    format: human
  - type: syslog
    level: info
    retry_interval: 20s
    format: human
  - type: tcp
    level: debug
    format: json
    address: logserver.example.com:1234
  - type: tcp
    level: debug
    format: json
    address: encryptedlogserver.example.com:1234
    retry_interval: 20s 
    tls:
      ca: /etc/zrepl/log/ca.crt
      cert: /etc/zrepl/log/key.pem
      key: /etc/zrepl/log/cert.pem
  - type: "file"
    format: "text"
    hide_fields:
      - "span"
    level: "warn"
`)
	assert.Len(t, conf.Global.Logging, 5)
	assert.NotNil(t, (conf.Global.Logging)[3].Ret.(*TCPLoggingOutlet).TLS)
}

func TestDefaultLoggingOutlet(t *testing.T) {
	conf := testValidGlobalSection(t, "")
	assert.Len(t, conf.Global.Logging, 1)
	defLogger := conf.Global.Logging[0].Ret
	require.IsType(t, new(FileLoggingOutlet), defLogger)
	o := defLogger.(*FileLoggingOutlet)
	assert.Equal(t, "warn", o.Level)
	assert.Equal(t, "text", o.Format)

	conf = testValidGlobalSection(t, `
global:
  logging:
`)
	assert.Len(t, conf.Global.Logging, 1)
	assert.Equal(t, defLogger, conf.Global.Logging[0].Ret)
}

func TestPrometheusMonitoring(t *testing.T) {
	conf := testValidGlobalSection(t, `
global:
  monitoring:
    - type: prometheus
      listen: ':9811'
`)
	assert.NotEmpty(t, conf.Global.Monitoring)
	assert.NotZero(t, conf.Global.Monitoring[0])
}

func TestSyslogLoggingOutletFacility(t *testing.T) {
	tests := []struct {
		name     string
		facility string
		priority syslog.Priority
	}{
		{name: "default", priority: syslog.LOG_LOCAL0}, // default
		{facility: "kern", priority: syslog.LOG_KERN},
		{facility: "daemon", priority: syslog.LOG_DAEMON},
		{facility: "auth", priority: syslog.LOG_AUTH},
		{facility: "syslog", priority: syslog.LOG_SYSLOG},
		{facility: "lpr", priority: syslog.LOG_LPR},
		{facility: "news", priority: syslog.LOG_NEWS},
		{facility: "uucp", priority: syslog.LOG_UUCP},
		{facility: "cron", priority: syslog.LOG_CRON},
		{facility: "authpriv", priority: syslog.LOG_AUTHPRIV},
		{facility: "ftp", priority: syslog.LOG_FTP},
		{facility: "local0", priority: syslog.LOG_LOCAL0},
		{facility: "local1", priority: syslog.LOG_LOCAL1},
		{facility: "local2", priority: syslog.LOG_LOCAL2},
		{facility: "local3", priority: syslog.LOG_LOCAL3},
		{facility: "local4", priority: syslog.LOG_LOCAL4},
		{facility: "local5", priority: syslog.LOG_LOCAL5},
		{facility: "local6", priority: syslog.LOG_LOCAL6},
		{facility: "local7", priority: syslog.LOG_LOCAL7},
	}

	for _, tt := range tests {
		name := tt.name
		if name == "" {
			name = tt.facility
		}
		t.Run(name, func(t *testing.T) {
			var s string
			if tt.facility != "" {
				s = "facility: " + tt.facility
			}
			logcfg := fmt.Sprintf(`
global:
  logging:
    - type: syslog
      level: info
      format: human
      %s
`, s)
			conf := testValidGlobalSection(t, logcfg)
			assert.Len(t, conf.Global.Logging, 1)
			assert.Equal(t, SyslogFacility(tt.priority), (conf.Global.Logging)[0].Ret.(*SyslogLoggingOutlet).Facility)
		})
	}
}

func TestLoggingOutletEnumList_SetDefaults(t *testing.T) {
	e := LoggingOutletEnumList{}
	var i defaults.Setter = &e
	require.NotPanics(t, func() {
		i.SetDefaults()
		defLogger := e[0].Ret
		require.IsType(t, new(FileLoggingOutlet), defLogger)
		o := defLogger.(*FileLoggingOutlet)
		assert.Equal(t, "warn", o.Level)
	})
}
