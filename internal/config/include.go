package config

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

func includeYAML(base, filename string, value any) error {
	if filename == "" {
		return nil
	} else if !filepath.IsAbs(filename) && base != "" {
		filename = filepath.Join(filepath.Dir(base), filename)
	}

	f, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("opening %q: %w", filename, err)
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	if err := dec.Decode(value); err != nil {
		return fmt.Errorf("decoding %q: %w", filename, err)
	}
	return nil
}
