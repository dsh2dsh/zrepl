package config

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"go.yaml.in/yaml/v4"
)

func appendYAML[T []E, E any](base, pattern string, target T) (T, error) {
	if pattern == "" {
		return nil, nil
	} else if !filepath.IsAbs(pattern) && base != "" {
		pattern = filepath.Join(filepath.Dir(base), pattern)
	}

	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed glob %q: %w", pattern, err)
	} else if matches == nil && !hasMeta(pattern) {
		if _, err := os.Lstat(pattern); err != nil {
			return nil, fmt.Errorf("failed include %q: %w", pattern, err)
		}
	}

	for _, name := range matches {
		f, err := os.Open(name)
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("open %q: %w", name, err)
		}
		dec := yaml.NewDecoder(f)
		var v T
		if err := dec.Decode(&v); err != nil {
			return nil, fmt.Errorf("yaml unmarshal %q: %w", name, err)
		}
		f.Close()
		target = append(target, v...)
	}
	return target, nil
}

// hasMeta reports whether path contains any of the magic characters recognized
// by Match.
//
// Copied from stdlib (path/filepath/match.go)
func hasMeta(path string) bool {
	magicChars := `*?[`
	if runtime.GOOS != "windows" {
		magicChars = `*?[\`
	}
	return strings.ContainsAny(path, magicChars)
}
