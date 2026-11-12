package native

import (
	"os"
	"syscall"
)

func OpenStdio() (*os.File, *os.File, error) {
	open := func(fd int, name string) (*os.File, error) {
		var info syscall.Stat_t
		if err := syscall.Fstat(fd, &info); err != nil || (info.Mode&syscall.S_IFMT != syscall.S_IFIFO && info.Mode&syscall.S_IFMT != syscall.S_IFSOCK) {
			return nil, code("PIPE_REQUIRED")
		}
		copyFD, err := syscall.Dup(fd)
		if err != nil {
			return nil, code("PIPE_REQUIRED")
		}
		if err = syscall.SetNonblock(copyFD, true); err != nil {
			syscall.Close(copyFD)
			return nil, code("PIPE_REQUIRED")
		}
		file := os.NewFile(uintptr(copyFD), name)
		if file == nil {
			syscall.Close(copyFD)
			return nil, code("PIPE_REQUIRED")
		}
		return file, nil
	}
	in, err := open(0, "protocol-input")
	if err != nil {
		return nil, nil, err
	}
	out, err := open(1, "protocol-output")
	if err != nil {
		in.Close()
		return nil, nil, err
	}
	return in, out, nil
}
