package config

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSnapshotting(t *testing.T) {
	tmpl := `
jobs:
- name: foo
  type: push
  connect:
    type: local
    listener_name: foo
    client_identity: bar
  filesystems: {"<": true}
  %s
  pruning:
    keep_sender:
    - type: last_n
      count: 10
    keep_receiver:
    - type: last_n
      count: 10
`
	manual := `
  snapshotting:
    type: manual
`
	periodic := `
  snapshotting:
    type: periodic
    prefix: zrepl_
    timestamp_format: dense
    interval: 10m
`
	cron := `
  snapshotting:
    type: cron
    prefix: zrepl_
    timestamp_format: human
    cron: "10 * * * *"
`

	periodicDaily := `
  snapshotting:
    type: periodic
    prefix: zrepl_
    interval: 1d
`

	hooks := `
  snapshotting:
    type: periodic
    prefix: zrepl_
    interval: 10m
    hooks:
    - type: command
      path: /tmp/path/to/command
      filesystems: { "<": true }
    - type: command
      path: /tmp/path/to/command
      filesystems: { "zroot<": true, "<": false }
`

	fillSnapshotting := func(s string) string { return fmt.Sprintf(tmpl, s) }
	var c *Config

	t.Run("manual", func(t *testing.T) {
		c = testValidConfig(t, fillSnapshotting(manual))
		snm := c.Jobs[0].Ret.(*PushJob).Snapshotting.Ret.(*SnapshottingManual)
		assert.Equal(t, "manual", snm.Type)
	})

	t.Run("periodic", func(t *testing.T) {
		c = testValidConfig(t, fillSnapshotting(periodic))
		snp := c.Jobs[0].Ret.(*PushJob).Snapshotting.Ret.(*SnapshottingPeriodic)
		assert.Equal(t, "periodic", snp.Type)
		assert.Equal(t, 10*time.Minute, snp.Interval.Duration())
		assert.Equal(t, "zrepl_", snp.Prefix)
	})

	t.Run("periodicDaily", func(t *testing.T) {
		c = testValidConfig(t, fillSnapshotting(periodicDaily))
		snp := c.Jobs[0].Ret.(*PushJob).Snapshotting.Ret.(*SnapshottingPeriodic)
		assert.Equal(t, "periodic", snp.Type)
		assert.Equal(t, 24*time.Hour, snp.Interval.Duration())
		assert.Equal(t, "zrepl_", snp.Prefix)
		assert.Equal(t, "dense", snp.TimestampFormat)
	})

	t.Run("cron", func(t *testing.T) {
		c = testValidConfig(t, fillSnapshotting(cron))
		snp := c.Jobs[0].Ret.(*PushJob).Snapshotting.Ret.(*SnapshottingPeriodic)
		assert.Equal(t, "cron", snp.Type)
		assert.Equal(t, "zrepl_", snp.Prefix)
		assert.Equal(t, "human", snp.TimestampFormat)
	})

	t.Run("hooks", func(t *testing.T) {
		c = testValidConfig(t, fillSnapshotting(hooks))
		hs := c.Jobs[0].Ret.(*PushJob).Snapshotting.Ret.(*SnapshottingPeriodic).Hooks
		assert.Equal(t, hs[0].Ret.(*HookCommand).Filesystems["<"], true)
		assert.Equal(t, hs[1].Ret.(*HookCommand).Filesystems["zroot<"], true)
	})
}

func TestSnapshottingTimestampDefaults(t *testing.T) {
	tmpl := `
jobs:
- name: foo
  type: push
  connect:
    type: local
    listener_name: foo
    client_identity: bar
  filesystems: {"<": true}
  %s
  pruning:
    keep_sender:
    - type: last_n
      count: 10
    keep_receiver:
    - type: last_n
      count: 10
`

	periodic := `
  snapshotting:
    type: periodic
    prefix: zrepl_
    interval: 10m
`
	cron := `
  snapshotting:
    type: cron
    prefix: zrepl_
    cron: "10 * * * *"
`

	fillSnapshotting := func(s string) string { return fmt.Sprintf(tmpl, s) }
	var c *Config

	t.Run("periodic", func(t *testing.T) {
		c = testValidConfig(t, fillSnapshotting(periodic))
		snp := c.Jobs[0].Ret.(*PushJob).Snapshotting.Ret.(*SnapshottingPeriodic)
		assert.Equal(t, "periodic", snp.Type)
		assert.Equal(t, 10*time.Minute, snp.Interval.Duration())
		assert.Equal(t, "zrepl_", snp.Prefix)
		assert.Equal(t, "dense", snp.TimestampFormat) // default was set correctly
	})

	t.Run("cron", func(t *testing.T) {
		c = testValidConfig(t, fillSnapshotting(cron))
		snp := c.Jobs[0].Ret.(*PushJob).Snapshotting.Ret.(*SnapshottingPeriodic)
		assert.Equal(t, "cron", snp.Type)
		assert.Equal(t, "zrepl_", snp.Prefix)
		assert.Equal(t, "dense", snp.TimestampFormat) // default was set correctly
	})
}
