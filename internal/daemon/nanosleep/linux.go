//go:build linux

package nanosleep

import "golang.org/x/sys/unix"

//nolint:wrapcheck // do not wrap unix err
func clockNanosleep(clockid int32, flags int, request *unix.Timespec,
	remain *unix.Timespec,
) error {
	return unix.ClockNanosleep(clockid, flags, request, remain)
}
