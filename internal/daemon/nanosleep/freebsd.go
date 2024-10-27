//go:build freebsd

package nanosleep

import (
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Do the interface allocations only once for common
// Errno values.
var (
	errEAGAIN error = syscall.EAGAIN
	errEINVAL error = syscall.EINVAL
	errENOENT error = syscall.ENOENT
)

// A copy of linux implementation, because [golang.org/x/sys/unix] doesn't have
// it for FreeBSD.
func clockNanosleep(clockid int32, flags int, request *unix.Timespec,
	remain *unix.Timespec,
) error {
	_, _, e1 := unix.Syscall6(unix.SYS_CLOCK_NANOSLEEP,
		uintptr(clockid), uintptr(flags),
		uintptr(unsafe.Pointer(request)),
		uintptr(unsafe.Pointer(remain)),
		0, 0)
	if e1 != 0 {
		return errnoErr(e1)
	}
	return nil
}

// errnoErr returns common boxed Errno values, to prevent
// allocations at runtime.
func errnoErr(e syscall.Errno) error {
	switch e {
	case 0:
		return nil
	case unix.EAGAIN:
		return errEAGAIN
	case unix.EINVAL:
		return errEINVAL
	case unix.ENOENT:
		return errENOENT
	}
	return e
}
