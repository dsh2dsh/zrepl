# zrepl

zrepl is a one-stop ZFS backup & replication solution.

This project is a fork of [zrepl](https://github.com/zrepl/zrepl).

FreeBSD port is
[here](https://github.com/dsh2dsh/freebsd-ports/tree/master/sysutils/zrepl-dsh2dsh).

## Changes from [upstream](https://github.com/zrepl/zrepl):

  * Go 1.22 and updated dependencies

  * ~~`last_n` keep rule fixed. See
    [#691](https://github.com/zrepl/zrepl/pull/750)~~ Merged.

  * Added support of shell patterns for datasets definitions. See
    [#755](https://github.com/zrepl/zrepl/pull/755). Configuration example:

    ``` yaml
    filesystems:
      "zroot/bastille/jails</*/root": true
    ```

    This configuration includes `zroot/bastille/jails/a/root`,
    `zroot/bastille/jails/b/root` zfs datasets and so on, and excludes
    `zroot/bastille/jails/a`, `zroot/bastille/jails/b` zfs datasets and so on.

    See [Match](https://pkg.go.dev/path/filepath@go1.22.0#Match) for details
    about patterns.

  * Added new log formatters: `json` and `text`. Both formatters use
    [slog](https://pkg.go.dev/log/slog) for formatting log entries. The new
    `json` formatter replaces old `json` formatter. Configuration example:

    ``` yaml
    logging:
      - type: "file"
        format: "text"            # or "json"
        time: false               # don't prepend with date and time
        hide_fields:
          - "span"                # don't log "span" field
    ```

  * Added ability to log into a file. See
    [#756](https://github.com/zrepl/zrepl/pull/756). Configuration example:

    ``` yaml
    logging:
      - type: "file"
        format: "text"            # or "json"
        time: false               # don't prepend with date and time
        hide_fields: &hide-log-fields
          - "span"                # don't log "span" field
        level:  "error"           # log errors only
        # without filename logs to stderr
      - type: "file"
        format: "text"
        hide_fields: *hide-log-fields
        level:  "info"
        filename: "/var/log/zrepl.log"
    ```

  * Replication jobs (without periodic snapshotting) can be configured for
    periodic run. See [#758](https://github.com/zrepl/zrepl/pull/758).
    Configuration example:

    ``` yaml
    - name: "zroot-to-server"
      type: "push"
      interval: "1h"
      snapshotting:
        type: "manual"
    ```

    Both `pull` and `push` job types support configuration of periodic run using
    cron specification. For instance:

    ``` yaml
    - name: "zroot-to-server"
      type: "push"
      cron: "25 15-22 * * *"
      snapshotting:
        type: "manual"
    ```

    See [CRON Expression Format] for details.

    [CRON Expression Format]: https://pkg.go.dev/github.com/robfig/cron/v3#hdr-CRON_Expression_Format

  * Added ability to configure command pipelines between `zfs send` and `zfs
    recv`. See [#761](https://github.com/zrepl/zrepl/pull/761). Configuration
    example:

    ``` yaml
    send:
      execpipe:
        # zfs send | zstd | mbuffer
        - [ "zstd", "-3" ]
        - [ "/usr/local/bin/mbuffer", "-q", "-s", "128k", "-m", "100M" ]
    ```

    ``` yaml
    recv:
      execpipe:
        # mbuffer | unzstd | zfs receive
        - [ "/usr/local/bin/mbuffer", "-q", "-s", "128k", "-m", "100M" ]
        - [ "unzstd" ]
    ```

  * Added Icinga/Nagios checks for checking the daemon is alive, latest or
    oldest snapshots are not too old. See
    [#765](https://github.com/zrepl/zrepl/pull/765). Configuration example:

    ``` yaml
    monitor:
      latest:
        - prefix: "zrepl_frequently_"
          critical: "48h"       # 2d
        - prefix: "zrepl_hourly_"
          critical: "48h"
        - prefix: "zrepl_daily_"
          critical: "48h"
        - prefix: "zrepl_monthly_"
          critical: "768h"      # 32d
      oldest:
        - prefix: "zrepl_frequently_"
          critical: "48h"       # 2d
        - prefix: "zrepl_hourly_"
          critical: "168h"      # 7d
        - prefix: "zrepl_daily_"
          critical: "2208h"     # 90d + 2d
        - prefix: "zrepl_monthly_"
          critical: "8688h"     # 30 * 12 = 360d + 2d
        - prefix: ""            # everything else
          critical: "168h"      # 7d
    ```

    Example of a daily script:

    ``` shell
    echo
    echo "zrepl status:"
    zrepl monitor alive
    zrepl monitor snapshots latest --job zdisk
    zrepl monitor snapshots oldest --job zdisk
    zrepl monitor snapshots latest --job zroot-to-zdisk
    zrepl monitor snapshots oldest --job zroot-to-zdisk
    ```

  * Removed support of `postgres-checkpoint` and `mysql-lock-tables` hooks.

  * Periodic snapshotting now recognizes cron specification. For instance:

    ``` yaml
    snapshotting:
      type: "periodic"
      cron: "25 15-22 * * *"
    ```

    `type: "cron"` still works too, just for compatibility. Both of them is the
    same type.

  * Fast skip "keep all" pruning.

    Instead of configuration like this:

    ``` yaml
    pruning:
      keep:
        - type: "regex"
          regex: ".*"
    ```

    or like this:

    ``` yaml
    pruning:
      keep_sender:
        - type: "regex"
          regex: ".*"
      keep_receiver:
    ```

    which keeps all snapshots, now it's possible to omit `pruning:` at all, or
    one of `keep_sender:` or `keep_receiver:`. In this case zrepl will early
    abort pruning and mark it as done.

    Originally zrepl requests all snapshots and does nothing after that, because
    pruning configured to keep all snapshots, but anyway it spends some time
    executing zfs commands.

  * Snapshots can be named using local time in timestamps, instead of UTC.
    Configuration example:

    ``` yaml
    snapshotting:
      type: "periodic"
      cron: "*/15 * * * *"
      prefix: "zrepl_frequently_"
      timestamp_format: "20060102_150405_MST"
      timestamp_local: true
    ```

    `timestamp_local` defines time zone of timestamps. By default it's `UTC`,
    but with `timestamp_local: true` it's local time zone. So instead of
    snapshot names like `zrepl_frequently_20240508_140000_000` it'll be
    something like `zrepl_frequently_20240508_160000_CEST`.

  * Configurable RPC timeout (1 minute by default). Configuration example:

    ``` yaml
    global:
      rpc_timeout: "2m30s"
    ```

    sets RPC timeout to 2 minutes and 30 seconds.

  * Configurable path to zfs binary ("zfs" by default). Configuration example:

    ``` yaml
    global:
      zfs_bin: "/sbin/zfs"
    ```

    sets zfs binary path to "/sbin/zfs".

  * Replication by default generates a stream package that sends all
    intermediary snapshots (`zfs send -I`), instead of every intermediary
    snapshot one by one (`zfs send -i`). If you want change it back to original
    slow mode:

    ``` yaml
    jobs:
      - name: "zroot-to-zdisk"
        type: "push"
        replication:
          # Send all intermediary snapshots as a stream package, instead of
          # sending them one by one. Default: true.
          one_step: false
    ```

    Replication with `one_step: true` is much faster. For instance a job on my
    desktop configured like:

    ``` yaml
    replication:
      concurrency:
        steps: 4
        size_estimates: 8
    ```

    replicates over WLAN for 1m32s, instead of 8m with configuration like

    ``` yaml
    replication:
      one_step: false
      concurrency:
        steps: 4
        size_estimates: 4
    ```

  * New command `zrepl signal stop`

    Stop the daemon right now. Actually it's the same like sending `SIGTERM` or
    `SIGINT` to the daemon.

  * New command `zrepl signal shutdown`

    Stop the daemon gracefully. After this signal, zrepl daemon will exit as
    soon as it'll be safe. It interrupts any operation, except replication
    steps. The daemon will wait for all replication steps completed and exit.

  * Added a configuration of file permissions for control socket. Example:

    ``` yaml
    global:
      control:
        # sockpath: "/var/run/zrepl/control"
        sockmode: 0o660             # write perm for group
    ```

    This configuration allows using of `zrepl status` for the group.

  * Small cosmetic changes

## Upstream user documentation

**User Documentation** can be found at
[zrepl.github.io](https://zrepl.github.io). Keep in mind, it doesn't contain
changes from this fork.
