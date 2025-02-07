package config

import (
	"errors"
	"fmt"
	"log/syslog"
	"time"

	"github.com/creasty/defaults"
	"gopkg.in/yaml.v3"

	zfsprop "github.com/dsh2dsh/zrepl/internal/zfs/property"
)

type Option func(self *Config)

func WithoutIncludes() Option {
	return func(self *Config) { self.skipIncludes = true }
}

func New(opts ...Option) *Config {
	c := new(Config)
	return c.init(opts...)
}

type Config struct {
	Global Global   `yaml:"global"`
	Listen []Listen `yaml:"listen" validate:"dive"`

	Keys        []AuthKey `yaml:"keys" validate:"dive"`
	IncludeKeys string    `yaml:"include_keys" validate:"omitempty,filepath"`

	Jobs        []JobEnum `yaml:"jobs" validate:"min=1,dive"`
	IncludeJobs string    `yaml:"include_jobs" validate:"omitempty,filepath"`

	skipIncludes bool
}

func (c *Config) init(opts ...Option) *Config {
	for _, fn := range opts {
		fn(c)
	}
	return c
}

func (c *Config) lateInit(path string) error {
	if len(c.Global.Logging) == 0 {
		c.Global.Logging.SetDefaults()
	} else if c.skipIncludes {
		return nil
	}

	if keys, err := appendYAML(path, c.IncludeKeys, c.Keys); err != nil {
		return err
	} else if keys != nil {
		c.Keys = keys
	}

	if jobs, err := appendYAML(path, c.IncludeJobs, c.Jobs); err != nil {
		return err
	} else if jobs != nil {
		c.Jobs = jobs
	}
	return nil
}

func (c *Config) Job(name string) (*JobEnum, error) {
	for i := range c.Jobs {
		j := &c.Jobs[i]
		if j.Name() == name {
			return j, nil
		}
	}
	return nil, fmt.Errorf("job %q not defined in config", name)
}

type AuthKey struct {
	Name string `yaml:"name" validate:"required"`
	Key  string `yaml:"key" validate:"required"`
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

func (j JobEnum) MonitorSnapshots() MonitorSnapshots {
	var m MonitorSnapshots
	switch v := j.Ret.(type) {
	case *SnapJob:
		m = v.MonitorSnapshots
	case *PushJob:
		m = v.MonitorSnapshots
	case *SinkJob:
		m = v.MonitorSnapshots
	case *PullJob:
		m = v.MonitorSnapshots
	case *SourceJob:
		m = v.MonitorSnapshots
	}
	return m
}

type ActiveJob struct {
	Type               string                   `yaml:"type" validate:"required"`
	Name               string                   `yaml:"name" validate:"required"`
	Connect            Connect                  `yaml:"connect"`
	Pruning            PruningSenderReceiver    `yaml:"pruning" validate:"required"`
	Replication        Replication              `yaml:"replication"`
	ConflictResolution ConflictResolution       `yaml:"conflict_resolution"`
	MonitorSnapshots   MonitorSnapshots         `yaml:"monitor"`
	Interval           PositiveDurationOrManual `yaml:"interval"`
	Cron               string                   `yaml:"cron"`
	Hooks              JobHooks                 `yaml:"hooks"`
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
	Count  []MonitorCount    `yaml:"count" validate:"dive"`
	Latest []MonitorCreation `yaml:"latest" validate:"dive"`
	Oldest []MonitorCreation `yaml:"oldest" validate:"dive"`
}

type MonitorCount struct {
	Prefix       string          `yaml:"prefix"`
	SkipDatasets []DatasetFilter `yaml:"skip_datasets" validate:"dive"`
	Warning      uint            `yaml:"warning"`
	Critical     uint            `yaml:"critical" validate:"required"`
}

type MonitorCreation struct {
	Prefix       string          `yaml:"prefix"`
	SkipDatasets []DatasetFilter `yaml:"skip_datasets" validate:"dive"`
	Warning      time.Duration   `yaml:"warning"`
	Critical     time.Duration   `yaml:"critical" validate:"required"`
}

func (self *MonitorSnapshots) Valid() bool {
	return len(self.Count) > 0 || len(self.Latest) > 0 ||
		len(self.Oldest) > 0
}

type JobHooks struct {
	Pre  *HookCommand `yaml:"pre"`
	Post *HookCommand `yaml:"post"`
}

type PassiveJob struct {
	Type             string           `yaml:"type" validate:"required"`
	Name             string           `yaml:"name" validate:"required"`
	ClientKeys       []string         `yaml:"client_keys" validate:"dive,required"`
	Pruning          PruningLocal     `yaml:"pruning"`
	MonitorSnapshots MonitorSnapshots `yaml:"monitor"`
}

type SnapJob struct {
	Type             string            `yaml:"type" validate:"required"`
	Name             string            `yaml:"name" validate:"required"`
	Pruning          PruningLocal      `yaml:"pruning"`
	Snapshotting     SnapshottingEnum  `yaml:"snapshotting"`
	Filesystems      FilesystemsFilter `yaml:"filesystems" validate:"required_without=Datasets"`
	Datasets         []DatasetFilter   `yaml:"datasets" validate:"required_without=Filesystems,dive"`
	MonitorSnapshots MonitorSnapshots  `yaml:"monitor"`
}

type DatasetFilter struct {
	Pattern   string `yaml:"pattern"`
	Exclude   bool   `yaml:"exclude"`
	Recursive bool   `yaml:"recursive" validate:"excluded_with=Shell"`
	Shell     bool   `yaml:"shell" validate:"excluded_with=Recursive"`
}

type SendOptions struct {
	ListPlaceholders bool `yaml:"list_placeholders"`
	Encrypted        bool `yaml:"encrypted"`
	Raw              bool `yaml:"raw" default:"true"`
	SendProperties   bool `yaml:"send_properties"`
	BackupProperties bool `yaml:"backup_properties"`
	LargeBlocks      bool `yaml:"large_blocks"`
	Compressed       bool `yaml:"compressed"`
	EmbeddedData     bool `yaml:"embedded_data"`
	Saved            bool `yaml:"saved"`

	ExecPipe [][]string `yaml:"execpipe" validate:"dive,required"`
}

type RecvOptions struct {
	// Note: we cannot enforce encrypted recv as the ZFS cli doesn't provide a
	// mechanism for it
	//
	// Encrypted bool `yaml:"may_encrypted"`
	// Future:
	// Reencrypt bool `yaml:"reencrypt"`

	Properties  PropertyRecvOptions    `yaml:"properties"`
	Placeholder PlaceholderRecvOptions `yaml:"placeholder"`

	ExecPipe [][]string `yaml:"execpipe" validate:"dive,required"`
}

type Replication struct {
	Protection  ReplicationOptionsProtection  `yaml:"protection"`
	Concurrency ReplicationOptionsConcurrency `yaml:"concurrency"`
	Prefix      string                        `yaml:"prefix"`
}

type ReplicationOptionsProtection struct {
	Initial     string `yaml:"initial" default:"guarantee_resumability" validate:"required"`
	Incremental string `yaml:"incremental" default:"guarantee_resumability" validate:"required"`
}

type ReplicationOptionsConcurrency struct {
	Steps         int  `yaml:"steps" default:"1" validate:"min=1"`
	SizeEstimates uint `yaml:"size_estimates"`
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
	Filesystems  FilesystemsFilter `yaml:"filesystems" validate:"required_without=Datasets"`
	Datasets     []DatasetFilter   `yaml:"datasets" validate:"required_without=Filesystems,dive"`
	Send         SendOptions       `yaml:"send"`
}

func (j *PushJob) GetFilesystems() (FilesystemsFilter, []DatasetFilter) {
	return j.Filesystems, j.Datasets
}

func (j *PushJob) GetSendOptions() *SendOptions { return &j.Send }

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
	Replication  Replication       `yaml:"replication"`
	Snapshotting SnapshottingEnum  `yaml:"snapshotting"`
	Filesystems  FilesystemsFilter `yaml:"filesystems" validate:"required_without=Datasets"`
	Datasets     []DatasetFilter   `yaml:"datasets" validate:"required_without=Filesystems,dive"`
	Send         SendOptions       `yaml:"send"`
}

func (j *SourceJob) GetFilesystems() (FilesystemsFilter, []DatasetFilter) {
	return j.Filesystems, j.Datasets
}

func (j *SourceJob) GetSendOptions() *SendOptions { return &j.Send }

type FilesystemsFilter map[string]bool

type SnapshottingEnum struct {
	Ret any `validate:"required"`
}

type SnapshottingPeriodic struct {
	Type            string        `yaml:"type" validate:"required"`
	Prefix          string        `yaml:"prefix" validate:"required"`
	Interval        Duration      `yaml:"interval"`
	Cron            string        `yaml:"cron"`
	Hooks           []HookCommand `yaml:"hooks" validate:"dive"`
	TimestampFormat string        `yaml:"timestamp_format" default:"dense" validate:"required"`
	TimestampLocal  bool          `yaml:"timestamp_local" default:"true"`
	Concurrency     uint          `yaml:"concurrency"`
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
	Concurrency  uint          `yaml:"concurrency"`
	KeepSender   []PruningEnum `yaml:"keep_sender"`
	KeepReceiver []PruningEnum `yaml:"keep_receiver"`
}

type PruningLocal struct {
	Concurrency uint          `yaml:"concurrency"`
	Keep        []PruningEnum `yaml:"keep"`
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

	Logging    LoggingOutletEnumList  `yaml:"logging" validate:"min=1"`
	Monitoring []PrometheusMonitoring `yaml:"monitoring" validate:"dive"`
	Control    GlobalControl          `yaml:"control"`
}

type Connect struct {
	Type           string `yaml:"type" validate:"required,oneof=http local"`
	Server         string `yaml:"server" validate:"required_if=Type http,omitempty,url"`
	ListenerName   string `yaml:"listener_name" validate:"required"`
	ClientIdentity string `yaml:"client_identity" validate:"required"`
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
	SockPath string `yaml:"sockpath" default:"/var/run/zrepl/control" validate:"filepath"`
	SockMode uint32 `yaml:"sockmode" validate:"lte=0o777"`
}

type HookCommand struct {
	Path        string            `yaml:"path" validate:"required"`
	Args        []string          `yaml:"args" validate:"dive,required"`
	Env         map[string]string `yaml:"env" validate:"dive,keys,required,endkeys,required"`
	Timeout     time.Duration     `yaml:"timeout" default:"30s" validate:"min=0s"`
	Filesystems FilesystemsFilter `yaml:"filesystems"`
	Datasets    []DatasetFilter   `yaml:"datasets" validate:"dive"`
	ErrIsFatal  bool              `yaml:"err_is_fatal"`
}

func (self *HookCommand) UnmarshalYAML(value *yaml.Node) error {
	type hookCommand HookCommand
	v := (*hookCommand)(self)
	if err := value.Decode(v); err != nil {
		return fmt.Errorf("UnmarshalYAML %T: %w", self, err)
	} else if err := defaults.Set(v); err != nil {
		return fmt.Errorf("set defaults for %T: %w", self, err)
	}
	return nil
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

var _ yaml.Unmarshaler = (*SyslogFacility)(nil)

func (t *SyslogFacility) UnmarshalYAML(value *yaml.Node) (err error) {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	return t.UnmarshalJSON([]byte(s))
}
