package config

type Listen struct {
	Addr string `yaml:"addr" validate:"required_without=Unix,omitempty,hostname_port"`

	Unix     string `yaml:"unix" validate:"required_without=Addr,omitempty,filepath"`
	UnixMode uint32 `yaml:"unix_mode" validate:"lte=0o777"`

	TLSCert string `yaml:"tls_cert" validate:"required_with=TLSKey,omitempty,filepath"`
	TLSKey  string `yaml:"tls_key" validate:"omitempty,filepath"`

	Control bool `yaml:"control" validate:"required_without=Metrics"`
	Metrics bool `yaml:"metrics" validate:"required_without=Control"`
}
