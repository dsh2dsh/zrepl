package config

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/creasty/defaults"
	"github.com/go-playground/validator/v10"
	"gopkg.in/yaml.v3"

	"github.com/dsh2dsh/zrepl/internal/config/env"
)

var configFileDefaultLocations = [...]string{
	"/etc/zrepl/zrepl.yml",
	"/usr/local/etc/zrepl/zrepl.yml",
}

func ParseConfig(path string, opts ...Option) (*Config, error) {
	if path == "" {
		// Try default locations
		for _, l := range configFileDefaultLocations {
			stat, statErr := os.Stat(l)
			if statErr != nil {
				continue
			}
			if !stat.Mode().IsRegular() {
				return nil, fmt.Errorf(
					"file at default location is not a regular file: %s", l)
			}
			path = l
			break
		}
	}

	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading %q: %w", path, err)
	}
	return ParseConfigBytes(path, b, opts...)
}

func ParseConfigBytes(path string, bytes []byte, opts ...Option,
) (*Config, error) {
	c := New(opts...)
	if err := defaults.Set(c); err != nil {
		return nil, fmt.Errorf("init config with defaults: %w", err)
	} else if err := yaml.Unmarshal(bytes, &c); err != nil {
		return nil, fmt.Errorf("config unmarshal: %w", err)
	} else if c == nil {
		return nil, errors.New("There was no yaml document in the file")
	}

	if err := c.lateInit(path); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	} else if err := Validator().Struct(c); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	} else if err := env.Parse(); err != nil {
		return nil, err
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
