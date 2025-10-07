package config

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v4"
)

type RetentionIntervalList []RetentionInterval

type PruneGrid struct {
	Type  string                `yaml:"type"`
	Grid  RetentionIntervalList `yaml:"grid"`
	Regex string                `yaml:"regex"`
}

type RetentionInterval struct {
	length    time.Duration
	keepCount int
}

func (i *RetentionInterval) Length() time.Duration {
	return i.length
}

func (i *RetentionInterval) KeepCount() int {
	return i.keepCount
}

const RetentionGridKeepCountAll int = -1

var _ yaml.Unmarshaler = (*RetentionIntervalList)(nil)

func (t *RetentionIntervalList) UnmarshalYAML(value *yaml.Node) (err error) {
	var in string
	if err := value.Decode(&in); err != nil {
		return fmt.Errorf("config: %w", err)
	}

	intervals, err := ParseRetentionIntervalSpec(in)
	if err != nil {
		return err
	}

	*t = intervals

	return nil
}

var retentionStringIntervalRegex *regexp.Regexp = regexp.MustCompile(`^\s*(\d+)\s*x\s*([^\(]+)\s*(\((.*)\))?\s*$`)

func parseRetentionGridIntervalString(e string) (intervals []RetentionInterval, err error) {
	comps := retentionStringIntervalRegex.FindStringSubmatch(e)
	if comps == nil {
		return intervals, errors.New(
			"retention string does not match expected format")
	}

	times, err := strconv.Atoi(comps[1])
	if err != nil {
		return nil, fmt.Errorf("parse %q to int: %w", comps[1], err)
	} else if times <= 0 {
		return nil, errors.New("contains factor <= 0")
	}

	duration, err := parsePositiveDuration(comps[2])
	if err != nil {
		return nil, err
	}

	keepCount := 1
	if comps[3] != "" {
		// Decompose key=value, comma separated
		// For now, only keep_count is supported
		re := regexp.MustCompile(`^\s*keep=(.+)\s*$`)
		res := re.FindStringSubmatch(comps[4])
		if res == nil || len(res) != 2 {
			return intervals, errors.New(
				"interval parameter contains unknown parameters")
		}
		if res[1] == "all" {
			keepCount = RetentionGridKeepCountAll
		} else {
			keepCount, err = strconv.Atoi(res[1])
			if err != nil {
				return nil, errors.New("cannot parse keep_count value")
			}
		}
	}

	intervals = make([]RetentionInterval, times)
	for i := range intervals {
		intervals[i] = RetentionInterval{
			length:    duration,
			keepCount: keepCount,
		}
	}
	return intervals, nil
}

func ParseRetentionIntervalSpec(s string) (intervals []RetentionInterval, err error) {
	ges := strings.Split(s, "|")
	intervals = make([]RetentionInterval, 0, 7*len(ges))

	for intervalIdx, e := range ges {
		parsed, err := parseRetentionGridIntervalString(e)
		if err != nil {
			return nil, fmt.Errorf("cannot parse interval %d of %d: %s: %s", intervalIdx+1, len(ges), err.Error(), strings.TrimSpace(e))
		}
		intervals = append(intervals, parsed...)
	}
	return intervals, nil
}
