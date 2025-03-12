# zrepl

zrepl is a one-stop ZFS backup & replication solution.

This project is a fork of [zrepl](https://github.com/zrepl/zrepl).

Current FreeBSD port is
[here](https://github.com/dsh2dsh/freebsd-ports/tree/master/sysutils/zrepl-dsh2dsh).
Keep in mind, sometimes it is a development version of this project, for
testing.

Stable version of this project can be easy installed on FreeBSD using

``` shell
pkg install zrepl-dsh2dsh
```

## Breaking changes!!!

  * The project has switched from gRPC to REST API.

    This change isn't compatible with old job configurations. Instead of
    configuring `serv:` for every job, it configures in one place:

    ```yaml
    # Include file with keys for accessing remote jobs and authenticate remote
    # clients. The filename is relative to filename of this configuration file.
    include_keys: "keys.yaml"
    # Include multiple files with keys from directory.
    #include_keys "keys.d/*.yaml"

    listen:
      # Serve "sink" and "source" jobs for network access.
      - addr: ":8888"
        tls_cert: "/usr/local/etc/ssl/cert.pem"
        tls_key: "/usr/local/etc/ssl/key.pem"
        zfs: true
    ```

    This configuration serves http and https API requests. `tls_cert` and
    `tls_key` are optional and needed for serving https requests.

    `keys.yaml` contains authentication keys of remote clients:

    ```yaml
    # Clients with defined authentication keys have network access to "sink" and
    # "source" jobs. The key name is their client identity name.

    # Authentication token and client_identity for me.
    - name: "a.domain.com"          # client_identity
      key: "long and secret token"
    ```

    By default all authenticated clients have remote access to `sink` and
    `source` jobs. But it can be restricted using `client_keys` like:

    ```yaml
    jobs:
      - name: "zdisk"
        type: "sink"
        # Restrict access to this job for listed remote clients
        client_keys:
          - "key1"
          - "key2"
          # and nobody else.
    ```

  * All transports has been replaced by `local` and `http` transports.

    `local` transport configuration looks almost the same:

    ```yaml
    jobs:
      - name: "zroot-to-zdisk"
        type: "push"
        connect:
          type: "local"
          listener_name: "zdisk"
          client_identity: "localhost"
    ```

    with one exception. `listener_name` now is a remote job name actually.

    The new `http` transport replaced all network transports. Its configuration
    look like:

    ```yaml
    jobs:
      - name: "zroot-to-server"
        type: "push"
        connect:
          type: "http"
          server: "https://server:8888"
          listener_name: "zdisk"
          client_identity: "serverkey"

      - name: "server-to-zdisk"
        type: "pull"
        connect:
          type: "http"
          server: "https://server:8888"
          listener_name: "zroot-to-client"
          client_identity: "serverkey"
    ```

    `listener_name` is a job name on the server with type of `sink` or `source`.

    `client_identity` is a key name from `keys.yaml`. That key will be sent to
    the server for authentication and the server must have a key with the same
    `key` content in `keys.yaml`. `name` can be different, because `sink` and
    `source` jobs use key name as `client_identity`.

## Changes from [upstream](https://github.com/zrepl/zrepl):

  * Fresh dependencies

  * ~~`last_n` keep rule fixed. See
    [#691](https://github.com/zrepl/zrepl/pull/750)~~ Merged.

  * Some of resolved upstream issues:

    * zrepl/zrepl#86
    * zrepl/zrepl#170
    * zrepl/zrepl#287
    * zrepl/zrepl#324
    * zrepl/zrepl#394
    * zrepl/zrepl#422
    * zrepl/zrepl#498
    * zrepl/zrepl#507
    * zrepl/zrepl#708
    * zrepl/zrepl#717
    * zrepl/zrepl#745
    * zrepl/zrepl#748

  * New dataset filter syntax instead of `filesystems`:

    New field `datasets` is a list of patterns. By default a pattern includes
    matched dataset. All patterns applied in order and last matched pattern
    wins. Lets see some examples.

    The following configuration will allow access to all datasets:

    ```yaml
    jobs:
      - name: "source"
        type: "source"
        datasets:
          - recursive: true
    ```

    The following configuration will allow access to datasets
    `zroot/ROOT/default` and `zroot/usr/home` including all their children.

    ```yaml
    jobs:
      - name: "snap-1h"
        type: "snap"
        datasets:
          - pattern: "zroot/ROOT/default"
            recursive: true
          - pattern: "zroot/usr/home"
            recursive: true
    ```

    The following configuration is more complicated:

    ```yaml
    jobs:
      - name: "source"
        type: "source"
        datasets:
          - pattern: "tank"         # rule (1)
            recursive: true
          - pattern: "tank/foo"     # rule (2)
            exclude: true
            recursive: true
          - pattern: "tank/foo/bar" # rule (3)
    ```

    `tank/foo/bar/loo` is excluded by (2), because (3) isn't matched (it isn't
    recursive).

    `tank/bar` is included by (1).

    `tank/foo/bar` is included by (3), because yes, it matched by (2), but last
    matched rule wins and (3) is the last matched rule.

    `zroot` isn't included at all, because nothing matched it.

    `tank/var/log` is included by (1), becuase this rule is recursive and other
    rules are not matched.

    For compatibility reasons old `filesystems` still works, but I wouldn't
    suggest use it. It's deprecated and can be removed anytime.

  * Added support of shell patterns for datasets definitions. Configuration
    example:

    ```yaml
    datasets:
      # exclude all children of zroot/bastille/jails
      - pattern: "zroot/bastille/jails"
        exclude: true
        recursive: true
      # except datasets matched by this shell pattern
      - pattern: "zroot/bastille/jails/*/root"
        shell: true
    ```

    This configuration includes `zroot/bastille/jails/a/root`,
    `zroot/bastille/jails/b/root` zfs datasets, and excludes
    `zroot/bastille/jails/a`, `zroot/bastille/jails/b` zfs datasets on.

    Another example:

    ```yaml
    datasets:
      # exclude datasets matched by this shell pattern
      - pattern: "zroot/bastille/jails/*/root"
        exclude: true
        shell: true
      # and include everything else inside zroot/bastille/jails
      - pattern: "zroot/bastille/jails"
        recursive: true
    ```

    excludes `zroot/bastille/jails/a/root`, `zroot/bastille/jails/b/root` and
    includes everything else inside `zroot/bastille/jails`.

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

  * Added Icinga/Nagios checks for checking the daemon is alive, snapshots count
    is ok, latest or oldest snapshots are not too old. See
    [#765](https://github.com/zrepl/zrepl/pull/765). Configuration example:

    ```yaml
    monitor:
      count:
        - prefix: "zrepl_frequently_"
          warning: 20
          critical: 30
        - prefix: "zrepl_hourly_"
          warning: 31
          critical: 50
        - prefix: "zrepl_daily_"
          warning: 91
          critical: 92
        - prefix: "zrepl_monthly_"
          warning: 13
          critical: 14
        - prefix: ""            # everything else
          warning: 2
          critical: 10
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

    Every item can be configured to skip some datasets from the check, like:

    ```yaml
    - prefix: "zrepl_monthly_"
      skip_datasets:
        - pattern: "zdisk/video"
      warning: 13
      critical: 14
    ```

    In this example it checks number of snapshots with prefix `zrepl_monthly_`
    for every dataset, configured in `datasets`, except `zdisk/video`.
    `skip_datasets` has the same syntax, like `datasets`.

    An example of a daily script:

    ```shell
    echo
    echo "zrepl status:"
    zrepl monitor alive
    zrepl monitor snapshots
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

  * Snapshots are named using local time for timestamps, instead of UTC.

    So instead of snapshot names like `zrepl_20240508_140000_000` it's
    `zrepl_20240508_160000_CEST`. `timestamp_local` defines time zone of
    timestamps. By default it's local time, but with `timestamp_local: false`
    it's UTC. Configuration like:

    ``` yaml
    snapshotting:
      type: "periodic"
      cron: "*/15 * * * *"
      prefix: "zrepl_"
      timestamp_format: "20060102_150405_000"
      timestamp_local: false
    ```

    returns original naming like `zrepl_20240508_140000_000` with UTC time.

  * Configurable RPC timeout (1 minute by default). Configuration example:

    ``` yaml
    global:
      rpc_timeout: "2m30s"
    ```

    sets RPC timeout to 2 minutes and 30 seconds.

    See also zrepl/zrepl#791

  * Configurable path to zfs binary ("zfs" by default). Configuration example:

    ``` yaml
    global:
      zfs_bin: "/sbin/zfs"
    ```

    sets zfs binary path to "/sbin/zfs".

  * Replication now generates a stream package that sends all intermediary
    snapshots (`zfs send -I`), instead of every intermediary snapshot one by one
    (`zfs send -i`). Such replication is much faster. For instance a replication
    job on my desktop configured like:

    ``` yaml
    replication:
      concurrency:
        steps: 4
        size_estimates: 8
    ```

    replicates over WLAN for 1m32s, instead of 8m.

  * New command `zrepl signal stop`

    Stop the daemon right now. Actually it's the same like sending `SIGINT` to
    the daemon.

  * New command `zrepl signal shutdown`

    Stop the daemon gracefully. After this signal, zrepl daemon will exit as
    soon as it'll be safe. It interrupts any operation, except replication
    steps. The daemon will wait for all replication steps completed and exit.

    Sending `SIGTERM` has the same effect.

  * Redesigned `zrepl status`

  * `zfs send -w` is default now. Example how to change it back:

    ``` yaml
    send:
      raw: false
    ```

  * New configuration for control and prometheus services. Example:

    ``` yaml
    listen:
      # control socket for zrepl client, like `zrepl signal` or `zrepl status`.
      - unix: "/var/run/zrepl/control"
        # unix_mode: 0o660            # write perm for group
        control: true

      # Export Prometheus metrics on http://127.0.0.1:8000/metrics
      - addr: "127.0.0.1:8000"
        # tls_cert: "/usr/local/etc/zrepl/cert.pem"
        # tls_key: "/usr/local/etc/zrepl/key.pem"
        metrics: true
    ```

    One of `addr` or `unix` is required or both of them can be configured. One
    of `control` or `metrics` is required or both of them can be configured too.
    Everything else is optional. For backward compatibility old style
    configuration works too.

    See also zrepl/zrepl#780

  * New optional `pre` and `post` hooks for `push` and `pull` jobs. Example:

    ```yaml
    - name: "zroot-to-zdisk"
      type: "push"
      hooks:
        pre:
          path: "/root/bin/zrepl_hook.sh"
          args: [ "pre" ]         # optional positional parameters
          env:                    # optional environment variables
            ZREPL_FOOBAR: "foo"
          timeout: "30s"          # optional, default is 1m
          # don't continue job if exit status is nonzero (default: false)
          err_is_fatal: true
        post:
          path: "/root/bin/zrepl_hook.sh"
          args: [ "post" ]        # optional positional parameters
          env:                    # optional environment variables
            ZREPL_FOOBAR: "bar"
          timeout: "0s"           # without timeout at all
    ```

    This configuration runs `/root/bin/zrepl_hook.sh pre` before replication
    with environment variables:

    ```
    ZREPL_FOOBAR=foo
    ZREPL_JOB_NAME=zroot-to-zdisk
    ```

    If it exit with nonzero exit status the job will not continue. By default
    `err_is_fatal: false` and exit status is ignored.

    After pruning finished it runs `/root/bin/zrepl_hook.sh post` with
    environment variables:

    ```
    ZREPL_FOOBAR=bar
    ZREPL_JOB_ERR=
    ZREPL_JOB_NAME=zroot-to-zdisk
    ```

    The `post` hook sets `ZREPL_JOB_ERR` to the last error. It's empty if the
    job finished without errors.

  * The pruning now prunes filesystems concurrently.

    By default it uses the number of CPUs as concurrency limit and it can be
    changed in config:

    ```yaml
    jobs:
      - name: "zroot-to-zdisk"
        pruning:
          concurrency: 1
    ```

    That `concurrency` can be defined on both sides: local and remote. One side
    uses it to limit concurrent `zfs list` and other side uses `concurrency`
    from their config to limit concurrent `zfs destroy`.

  * Job configurations can be included from multiple files:

    ```yaml
    include_jobs: "jobs.d/*.yaml"
    ```

    Like `include_keys`, the directory is relative to main configuration file.

    `include_jobs` can be combined with `jobs`:

    ```yaml
    jobs:
      - name: "zroot-to-zdisk"
    include_jobs: "jobs.d/*.yaml"
    ```

  * The replication can be configured to replicate subset of snapshots, instead
    of all of them. Example:

    ``` yaml
    jobs:
      - name: "zroot-to-zdisk"
        type: "push"
        replication:
          prefix: "zrepl_"
    ```

    This configuration will replicate snapshots with names beginning with
    "zrepl_".

    See also zrepl/zrepl#403

  * Snapshots now created concurrently.

    By default it uses the number of CPUs as concurrency limit and it can be
    changed in config:

    ```yaml
    jobs:
      - name: "zroot-to-zdisk"
        snapshotting:
          type: "periodic"
          concurrency: 1
    ```

  * Recursive snapshots `zfs snaphot -r`

    Where possible, snapshots created using `zfs snaphot -r`. With configuration
    like this:

    ```yaml
    jobs:
      - name: "snap-1h"
        type: "snap"
        datasets:
          - pattern: "zroot"
            recursive: true
    ```

    `zfs snapshot -r zroot` recursively create snapshots of all descendent
    datasets, instead of one by one. But if recursive dataset has any exclusion,
    like this:

    ```yaml
    jobs:
      - name: "snap-1h"
        type: "snap"
        datasets:
          - pattern: "zroot"
            recursive: true
          - pattern: "zroot/foo"
            exclude: true
    ```

    `-r` not used and it create snapshots of all descendent datasets one by one.

    See also zrepl/zrepl#634

  * Faster `zfs list` (zrepl/zrepl#870)

    Runs `zfs list filesystem...`, if possible, and list only specific datasets,
    instead of `zfs list`, which lists every dataset on the entire system.

## Upstream user documentation

**User Documentation** can be found at
[zrepl.github.io](https://zrepl.github.io). Keep in mind, it doesn't contain
changes from this fork.
