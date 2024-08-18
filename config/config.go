package config

import (
	"errors"
	"fmt"
	"log/syslog"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/creasty/defaults"
	"github.com/go-playground/validator/v10"
	"gopkg.in/yaml.v3"

	"github.com/dsh2dsh/zrepl/util/datasizeunit"
	zfsprop "github.com/dsh2dsh/zrepl/zfs/property"
)

type ParseFlags uint

const (
	ParseFlagsNone        ParseFlags = 0
	ParseFlagsNoCertCheck ParseFlags = 1 << iota
)

func New() *Config {
	return new(Config)
}

type Config struct {
	Jobs   []JobEnum `yaml:"jobs" validate:"min=1,dive"`
	Global Global    `yaml:"global"`
}

func (c *Config) lateInit() {
	if len(c.Global.Logging) == 0 {
		c.Global.Logging.SetDefaults()
	}
}

func (c *Config) Job(name string) (*JobEnum, error) {
	for _, j := range c.Jobs {
		if j.Name() == name {
			return &j, nil
		}
	}
	return nil, fmt.Errorf("job %q not defined in config", name)
}

type JobEnum struct {
	Ret any `validate:"required"`
}

func (j JobEnum) Name() string {
	var name string
	switch v := j.Ret.(type) {
	case *SnapJob:
		name = v.Name
	case *PushJob:
		name = v.Name
	case *SinkJob:
		name = v.Name
	case *PullJob:
		name = v.Name
	case *SourceJob:
		name = v.Name
	default:
		panic(fmt.Sprintf("unknown job type %T", v))
	}
	return name
}

type ActiveJob struct {
	Type               string                   `yaml:"type" validate:"required"`
	Name               string                   `yaml:"name" validate:"required"`
	Connect            ConnectEnum              `yaml:"connect"`
	Pruning            PruningSenderReceiver    `yaml:"pruning" validate:"required"`
	Replication        Replication              `yaml:"replication"`
	ConflictResolution ConflictResolution       `yaml:"conflict_resolution"`
	MonitorSnapshots   MonitorSnapshots         `yaml:"monitor"`
	Interval           PositiveDurationOrManual `yaml:"interval"`
	Cron               string                   `yaml:"cron"`
}

func (self *ActiveJob) CronSpec() string {
	if self.Cron != "" {
		return self.Cron
	} else if self.Interval.Interval > 0 && !self.Interval.Manual {
		return "@every " + self.Interval.Interval.Truncate(time.Second).String()
	}
	return ""
}

type ConflictResolution struct {
	InitialReplication string `yaml:"initial_replication" default:"all" validate:"required"`
}

type MonitorSnapshots struct {
	Latest []MonitorSnapshot `yaml:"latest" validate:"dive"`
	Oldest []MonitorSnapshot `yaml:"oldest" validate:"dive"`
}

type MonitorSnapshot struct {
	Prefix   string        `yaml:"prefix"`
	Warning  time.Duration `yaml:"warning"`
	Critical time.Duration `yaml:"critical" validate:"required"`
}

type PassiveJob struct {
	Type             string           `yaml:"type" validate:"required"`
	Name             string           `yaml:"name" validate:"required"`
	Serve            ServeEnum        `yaml:"serve"`
	MonitorSnapshots MonitorSnapshots `yaml:"monitor"`
}

type SnapJob struct {
	Type             string            `yaml:"type" validate:"required"`
	Name             string            `yaml:"name" validate:"required"`
	Pruning          PruningLocal      `yaml:"pruning"`
	Snapshotting     SnapshottingEnum  `yaml:"snapshotting"`
	Filesystems      FilesystemsFilter `yaml:"filesystems" validate:"min=1"`
	MonitorSnapshots MonitorSnapshots  `yaml:"monitor"`
}

type SendOptions struct {
	Encrypted        bool `yaml:"encrypted"`
	Raw              bool `yaml:"raw"`
	SendProperties   bool `yaml:"send_properties"`
	BackupProperties bool `yaml:"backup_properties"`
	LargeBlocks      bool `yaml:"large_blocks"`
	Compressed       bool `yaml:"compressed"`
	EmbeddedData     bool `yaml:"embedded_data"`
	Saved            bool `yaml:"saved"`

	BandwidthLimit BandwidthLimit `yaml:"bandwidth_limit"`
	ExecPipe       [][]string     `yaml:"execpipe" validate:"dive,required"`
}

type RecvOptions struct {
	// Note: we cannot enforce encrypted recv as the ZFS cli doesn't provide a mechanism for it
	// Encrypted bool `yaml:"may_encrypted"`
	// Future:
	// Reencrypt bool `yaml:"reencrypt"`

	Properties     PropertyRecvOptions    `yaml:"properties"`
	BandwidthLimit BandwidthLimit         `yaml:"bandwidth_limit"`
	Placeholder    PlaceholderRecvOptions `yaml:"placeholder"`
	ExecPipe       [][]string             `yaml:"execpipe" validate:"dive,required"`
}

var _ yaml.Unmarshaler = &datasizeunit.Bits{}

type BandwidthLimit struct {
	Max            datasizeunit.Bits `yaml:"max" default:"-1 B" validate:"required"`
	BucketCapacity datasizeunit.Bits `yaml:"bucket_capacity" default:"128 KiB" validate:"required"`
}

type Replication struct {
	Protection  ReplicationOptionsProtection  `yaml:"protection"`
	Concurrency ReplicationOptionsConcurrency `yaml:"concurrency"`
	OneStep     bool                          `yaml:"one_step" default:"true"`
}

type ReplicationOptionsProtection struct {
	Initial     string `yaml:"initial" default:"guarantee_resumability" validate:"required"`
	Incremental string `yaml:"incremental" default:"guarantee_resumability" validate:"required"`
}

type ReplicationOptionsConcurrency struct {
	Steps         int `yaml:"steps" default:"1" validate:"min=1"`
	SizeEstimates int `yaml:"size_estimates" default:"4" validate:"min=1"`
}

type PropertyRecvOptions struct {
	Inherit  []zfsprop.Property          `yaml:"inherit" validate:"dive,required"`
	Override map[zfsprop.Property]string `yaml:"override" validate:"dive,required"`
}

type PlaceholderRecvOptions struct {
	Encryption string `yaml:"encryption" default:"inherit" validate:"required"`
}

type PushJob struct {
	ActiveJob    `yaml:",inline"`
	Snapshotting SnapshottingEnum  `yaml:"snapshotting"`
	Filesystems  FilesystemsFilter `yaml:"filesystems" validate:"min=1"`
	Send         SendOptions       `yaml:"send"`
}

func (j *PushJob) GetFilesystems() FilesystemsFilter { return j.Filesystems }
func (j *PushJob) GetSendOptions() *SendOptions      { return &j.Send }

type PullJob struct {
	ActiveJob `yaml:",inline"`
	RootFS    string      `yaml:"root_fs" validate:"required"`
	Recv      RecvOptions `yaml:"recv"`
}

func (j *PullJob) GetRootFS() string             { return j.RootFS }
func (j *PullJob) GetAppendClientIdentity() bool { return false }
func (j *PullJob) GetRecvOptions() *RecvOptions  { return &j.Recv }

type PositiveDurationOrManual struct {
	Interval time.Duration
	Manual   bool
}

var _ yaml.Unmarshaler = (*PositiveDurationOrManual)(nil)

func (i *PositiveDurationOrManual) UnmarshalYAML(value *yaml.Node) (err error) {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	switch s {
	case "manual":
		i.Manual = true
		i.Interval = 0
	case "":
		return errors.New("value must not be empty")
	default:
		i.Manual = false
		i.Interval, err = parsePositiveDuration(s)
		if err != nil {
			return err
		}
	}
	return nil
}

type SinkJob struct {
	PassiveJob `yaml:",inline"`
	RootFS     string      `yaml:"root_fs" validate:"required"`
	Recv       RecvOptions `yaml:"recv"`
}

func (j *SinkJob) GetRootFS() string             { return j.RootFS }
func (j *SinkJob) GetAppendClientIdentity() bool { return true }
func (j *SinkJob) GetRecvOptions() *RecvOptions  { return &j.Recv }

type SourceJob struct {
	PassiveJob   `yaml:",inline"`
	Snapshotting SnapshottingEnum  `yaml:"snapshotting"`
	Filesystems  FilesystemsFilter `yaml:"filesystems" validate:"min=1"`
	Send         SendOptions       `yaml:"send"`
}

func (j *SourceJob) GetFilesystems() FilesystemsFilter { return j.Filesystems }
func (j *SourceJob) GetSendOptions() *SendOptions      { return &j.Send }

type FilesystemsFilter map[string]bool

type SnapshottingEnum struct {
	Ret any `validate:"required"`
}

type SnapshottingPeriodic struct {
	Type            string   `yaml:"type" validate:"required"`
	Prefix          string   `yaml:"prefix" validate:"required"`
	Interval        Duration `yaml:"interval"`
	Cron            string   `yaml:"cron"`
	Hooks           HookList `yaml:"hooks"`
	TimestampFormat string   `yaml:"timestamp_format" default:"dense" validate:"required"`
	TimestampLocal  bool     `yaml:"timestamp_local" default:"true"`
}

func (self *SnapshottingPeriodic) CronSpec() string {
	if self.Cron != "" {
		return self.Cron
	} else if self.Interval.Duration() > 0 {
		return "@every " + self.Interval.Duration().Truncate(time.Second).String()
	}
	return ""
}

type SnapshottingManual struct {
	Type string `yaml:"type" validate:"required"`
}

type PruningSenderReceiver struct {
	KeepSender   []PruningEnum `yaml:"keep_sender"`
	KeepReceiver []PruningEnum `yaml:"keep_receiver"`
}

type PruningLocal struct {
	Keep []PruningEnum `yaml:"keep"`
}

type LoggingOutletEnumList []LoggingOutletEnum

func (l *LoggingOutletEnumList) SetDefaults() {
	s := new(FileLoggingOutlet)
	defaults.MustSet(s)
	err := yaml.Unmarshal([]byte(`
type: "file"
format: "text"
hide_fields:
  - "span"
level: "warn"
`), s)
	if err != nil {
		panic(err)
	}
	*l = []LoggingOutletEnum{{Ret: s}}
}

var _ defaults.Setter = &LoggingOutletEnumList{}

type Global struct {
	RpcTimeout time.Duration `yaml:"rpc_timeout" default:"1m" validate:"gt=0s"`
	ZfsBin     string        `yaml:"zfs_bin" default:"zfs" validate:"required"`

	Logging    LoggingOutletEnumList `yaml:"logging" validate:"min=1"`
	Monitoring []MonitoringEnum      `yaml:"monitoring"`
	Control    GlobalControl         `yaml:"control"`
	Serve      GlobalServe           `yaml:"serve"`
}

type ConnectEnum struct {
	Ret any `validate:"required"`
}

type ConnectCommon struct {
	Type string `yaml:"type" validate:"required"`
}

type TCPConnect struct {
	ConnectCommon `yaml:",inline"`
	Address       string        `yaml:"address" validate:"required,hostname_port"`
	DialTimeout   time.Duration `yaml:"dial_timeout" default:"10s" validate:"min=0s"`
}

type TLSConnect struct {
	ConnectCommon `yaml:",inline"`
	Address       string        `yaml:"address" validate:"required,hostname_port"`
	Ca            string        `yaml:"ca" validate:"required"`
	Cert          string        `yaml:"cert" validate:"required"`
	Key           string        `yaml:"key" validate:"required"`
	ServerCN      string        `yaml:"server_cn" validate:"required"`
	DialTimeout   time.Duration `yaml:"dial_timeout" default:"10s" validate:"min=0s"`
}

type SSHStdinserverConnect struct {
	ConnectCommon        `yaml:",inline"`
	Host                 string        `yaml:"host" validate:"required"`
	User                 string        `yaml:"user" validate:"required"`
	Port                 uint16        `yaml:"port" validate:"required"`
	IdentityFile         string        `yaml:"identity_file" validate:"required"`
	TransportOpenCommand []string      `yaml:"transport_open_command"` // TODO unused
	SSHCommand           string        `yaml:"ssh_command"`            // TODO unused
	Options              []string      `yaml:"options" validate:"dive,required"`
	DialTimeout          time.Duration `yaml:"dial_timeout" default:"10s" validate:"min=0s"`
}

type LocalConnect struct {
	ConnectCommon  `yaml:",inline"`
	ListenerName   string        `yaml:"listener_name" validate:"required"`
	ClientIdentity string        `yaml:"client_identity" validate:"required"`
	DialTimeout    time.Duration `yaml:"dial_timeout" default:"2s" validate:"min=0s"`
}

type ServeEnum struct {
	Ret any `validate:"required"`
}

type ServeCommon struct {
	Type string `yaml:"type" validate:"required"`
}

type TCPServe struct {
	ServeCommon    `yaml:",inline"`
	Listen         string            `yaml:"listen" validate:"required,hostname_port"`
	ListenFreeBind bool              `yaml:"listen_freebind"`
	Clients        map[string]string `yaml:"clients" validate:"dive,required"`
}

type TLSServe struct {
	ServeCommon      `yaml:",inline"`
	Listen           string        `yaml:"listen" validate:"required,hostname_port"`
	ListenFreeBind   bool          `yaml:"listen_freebind"`
	Ca               string        `yaml:"ca" validate:"required"`
	Cert             string        `yaml:"cert" validate:"required"`
	Key              string        `yaml:"key" validate:"required"`
	ClientCNs        []string      `yaml:"client_cns" validate:"dive,required"`
	HandshakeTimeout time.Duration `yaml:"handshake_timeout" default:"10s" validate:"min=0s"`
}

type StdinserverServer struct {
	ServeCommon      `yaml:",inline"`
	ClientIdentities []string `yaml:"client_identities" validate:"dive,required"`
}

type LocalServe struct {
	ServeCommon  `yaml:",inline"`
	ListenerName string `yaml:"listener_name" validate:"required"`
}

type PruningEnum struct {
	Ret any `validate:"required"`
}

type PruneKeepNotReplicated struct {
	Type                 string `yaml:"type" validate:"required"`
	KeepSnapshotAtCursor bool   `yaml:"keep_snapshot_at_cursor" default:"true"`
}

type PruneKeepLastN struct {
	Type  string `yaml:"type" validate:"required"`
	Count int    `yaml:"count" validate:"required"`
	Regex string `yaml:"regex"`
}

type PruneKeepRegex struct { // FIXME rename to KeepRegex
	Type   string `yaml:"type" validate:"required"`
	Regex  string `yaml:"regex" validate:"required"`
	Negate bool   `yaml:"negate"`
}

type LoggingOutletEnum struct {
	Ret any `validate:"required"`
}

type LoggingOutletCommon struct {
	Type       string   `yaml:"type" validate:"required"`
	Level      string   `yaml:"level" validate:"required"`
	Format     string   `yaml:"format" validate:"required"`
	HideFields []string `yaml:"hide_fields"`
	Time       bool     `yaml:"time" default:"true"`
}

type FileLoggingOutlet struct {
	LoggingOutletCommon `yaml:",inline"`
	FileName            string `yaml:"filename"`
}

type SyslogLoggingOutlet struct {
	LoggingOutletCommon `yaml:",inline"`
	Facility            SyslogFacility `yaml:"facility" default:"local0" validate:"required"`
	RetryInterval       time.Duration  `yaml:"retry_interval" default:"10s" validate:"gt=0s"`
}

type TCPLoggingOutlet struct {
	LoggingOutletCommon `yaml:",inline"`
	Address             string               `yaml:"address" validate:"required,hostname_port"`
	Net                 string               `yaml:"net" default:"tcp" validate:"required"`
	RetryInterval       time.Duration        `yaml:"retry_interval" default:"10s" validate:"gt=0s"`
	TLS                 *TCPLoggingOutletTLS `yaml:"tls"`
}

type TCPLoggingOutletTLS struct {
	CA   string `yaml:"ca" validate:"required"`
	Cert string `yaml:"cert" validate:"required"`
	Key  string `yaml:"key" validate:"required"`
}

type MonitoringEnum struct {
	Ret any `validate:"required"`
}

type PrometheusMonitoring struct {
	Type           string `yaml:"type" validate:"required"`
	Listen         string `yaml:"listen" validate:"required,hostname_port"`
	ListenFreeBind bool   `yaml:"listen_freebind"`
}

type SyslogFacility syslog.Priority

func (f *SyslogFacility) UnmarshalJSON(b []byte) error {
	s := string(b)
	var level syslog.Priority
	switch s {
	case "kern":
		level = syslog.LOG_KERN
	case "user":
		level = syslog.LOG_USER
	case "mail":
		level = syslog.LOG_MAIL
	case "daemon":
		level = syslog.LOG_DAEMON
	case "auth":
		level = syslog.LOG_AUTH
	case "syslog":
		level = syslog.LOG_SYSLOG
	case "lpr":
		level = syslog.LOG_LPR
	case "news":
		level = syslog.LOG_NEWS
	case "uucp":
		level = syslog.LOG_UUCP
	case "cron":
		level = syslog.LOG_CRON
	case "authpriv":
		level = syslog.LOG_AUTHPRIV
	case "ftp":
		level = syslog.LOG_FTP
	case "local0":
		level = syslog.LOG_LOCAL0
	case "local1":
		level = syslog.LOG_LOCAL1
	case "local2":
		level = syslog.LOG_LOCAL2
	case "local3":
		level = syslog.LOG_LOCAL3
	case "local4":
		level = syslog.LOG_LOCAL4
	case "local5":
		level = syslog.LOG_LOCAL5
	case "local6":
		level = syslog.LOG_LOCAL6
	case "local7":
		level = syslog.LOG_LOCAL7
	default:
		return fmt.Errorf("invalid syslog level: %q", s)
	}
	*f = SyslogFacility(level)
	return nil
}

func (f *SyslogFacility) SetDefaults() {
	*f = SyslogFacility(syslog.LOG_LOCAL0)
}

var _ defaults.Setter = (*SyslogFacility)(nil)

type GlobalControl struct {
	SockPath string `yaml:"sockpath" default:"/var/run/zrepl/control" validate:"required"`
	SockMode uint32 `yaml:"sockmode" validate:"lte=0o777"`
}

type GlobalServe struct {
	StdinServer *GlobalStdinServer `yaml:"stdinserver" default:"{}" validate:"required"`
}

type GlobalStdinServer struct {
	SockDir string `yaml:"sockdir" default:"/var/run/zrepl/stdinserver" validate:"required"`
}

type HookList []HookEnum

type HookEnum struct {
	Ret any `validate:"required"`
}

type HookCommand struct {
	Path               string            `yaml:"path" validate:"required"`
	Timeout            time.Duration     `yaml:"timeout" default:"30s" validate:"gt=0s"`
	Filesystems        FilesystemsFilter `yaml:"filesystems" validate:"min=1"`
	HookSettingsCommon `yaml:",inline"`
}

type HookSettingsCommon struct {
	Type       string `yaml:"type" validate:"required"`
	ErrIsFatal bool   `yaml:"err_is_fatal"`
}

func enumUnmarshal(value *yaml.Node, types map[string]any) (any, error) {
	var in struct {
		Type string `yaml:"type" validate:"required"`
	}
	if err := value.Decode(&in); err != nil {
		return nil, err
	} else if in.Type == "" {
		return nil, &yaml.TypeError{Errors: []string{"must specify type"}}
	}

	v, ok := types[in.Type]
	if !ok {
		return nil, &yaml.TypeError{
			Errors: []string{"invalid type name " + in.Type},
		}
	}

	if err := defaults.Set(v); err != nil {
		return nil, fmt.Errorf("set defaults for type %q: %w", in.Type, err)
	} else if err := value.Decode(v); err != nil {
		return nil, err
	}
	return v, nil
}

var _ yaml.Unmarshaler = (*JobEnum)(nil)

func (t *JobEnum) UnmarshalYAML(value *yaml.Node) (err error) {
	t.Ret, err = enumUnmarshal(value, map[string]any{
		"snap":   new(SnapJob),
		"push":   new(PushJob),
		"sink":   new(SinkJob),
		"pull":   new(PullJob),
		"source": new(SourceJob),
	})
	return
}

var _ yaml.Unmarshaler = (*ConnectEnum)(nil)

func (t *ConnectEnum) UnmarshalYAML(value *yaml.Node) (err error) {
	t.Ret, err = enumUnmarshal(value, map[string]any{
		"tcp":             new(TCPConnect),
		"tls":             new(TLSConnect),
		"ssh+stdinserver": new(SSHStdinserverConnect),
		"local":           new(LocalConnect),
	})
	return
}

var _ yaml.Unmarshaler = (*ServeEnum)(nil)

func (t *ServeEnum) UnmarshalYAML(value *yaml.Node) (err error) {
	t.Ret, err = enumUnmarshal(value, map[string]any{
		"tcp":         new(TCPServe),
		"tls":         new(TLSServe),
		"stdinserver": new(StdinserverServer),
		"local":       new(LocalServe),
	})
	return
}

var _ yaml.Unmarshaler = (*PruningEnum)(nil)

func (t *PruningEnum) UnmarshalYAML(value *yaml.Node) (err error) {
	t.Ret, err = enumUnmarshal(value, map[string]any{
		"not_replicated": new(PruneKeepNotReplicated),
		"last_n":         new(PruneKeepLastN),
		"grid":           new(PruneGrid),
		"regex":          new(PruneKeepRegex),
	})
	return
}

var _ yaml.Unmarshaler = (*SnapshottingEnum)(nil)

func (t *SnapshottingEnum) UnmarshalYAML(value *yaml.Node) (err error) {
	t.Ret, err = enumUnmarshal(value, map[string]any{
		"periodic": new(SnapshottingPeriodic),
		"manual":   new(SnapshottingManual),
		"cron":     new(SnapshottingPeriodic),
	})
	return
}

var _ yaml.Unmarshaler = (*LoggingOutletEnum)(nil)

func (t *LoggingOutletEnum) UnmarshalYAML(value *yaml.Node) (err error) {
	t.Ret, err = enumUnmarshal(value, map[string]any{
		"file":   new(FileLoggingOutlet),
		"stdout": new(FileLoggingOutlet),
		"syslog": new(SyslogLoggingOutlet),
		"tcp":    new(TCPLoggingOutlet),
	})
	return
}

var _ yaml.Unmarshaler = (*MonitoringEnum)(nil)

func (t *MonitoringEnum) UnmarshalYAML(value *yaml.Node) (err error) {
	t.Ret, err = enumUnmarshal(value, map[string]any{
		"prometheus": new(PrometheusMonitoring),
	})
	return
}

var _ yaml.Unmarshaler = (*SyslogFacility)(nil)

func (t *SyslogFacility) UnmarshalYAML(value *yaml.Node) (err error) {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	return t.UnmarshalJSON([]byte(s))
}

var _ yaml.Unmarshaler = (*HookEnum)(nil)

func (t *HookEnum) UnmarshalYAML(value *yaml.Node) (err error) {
	t.Ret, err = enumUnmarshal(value, map[string]any{
		"command": new(HookCommand),
	})
	return
}

var ConfigFileDefaultLocations = []string{
	"/etc/zrepl/zrepl.yml",
	"/usr/local/etc/zrepl/zrepl.yml",
}

func ParseConfig(path string) (i *Config, err error) {
	if path == "" {
		// Try default locations
		for _, l := range ConfigFileDefaultLocations {
			stat, statErr := os.Stat(l)
			if statErr != nil {
				continue
			}
			if !stat.Mode().IsRegular() {
				err = fmt.Errorf("file at default location is not a regular file: %s", l)
				return
			}
			path = l
			break
		}
	}

	var bytes []byte

	if bytes, err = os.ReadFile(path); err != nil {
		return
	}

	return ParseConfigBytes(bytes)
}

func ParseConfigBytes(bytes []byte) (*Config, error) {
	c := New()
	if err := defaults.Set(c); err != nil {
		return nil, fmt.Errorf("init config with defaults: %w", err)
	} else if err := yaml.Unmarshal(bytes, &c); err != nil {
		return nil, fmt.Errorf("config unmarshal: %w", err)
	} else if c == nil {
		return nil, errors.New("There was no yaml document in the file")
	}
	c.lateInit()

	if err := Validator().Struct(c); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}
	return c, nil
}

func Validator() *validator.Validate {
	if validate == nil {
		validate = newValidator()
	}
	return validate
}

var validate *validator.Validate

func newValidator() *validator.Validate {
	validate := validator.New(validator.WithRequiredStructEnabled())
	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		name := strings.SplitN(fld.Tag.Get("yaml"), ",", 2)[0]
		// skip if tag key says it should be ignored
		if name == "-" {
			return ""
		}
		return name
	})
	return validate
}
