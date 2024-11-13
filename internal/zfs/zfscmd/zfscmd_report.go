package zfscmd

import (
	"fmt"
	"sync"
	"time"
)

type Report struct {
	Active []ActiveCommand
}

type ActiveCommand struct {
	Path      string
	Args      []string
	StartedAt time.Time
}

func GetReport() *Report {
	active.mtx.RLock()
	defer active.mtx.RUnlock()
	activeCommands := make([]ActiveCommand, 0, len(active.cmds))
	for c := range active.cmds {
		c.mtx.RLock()
		activeCommands = append(activeCommands, ActiveCommand{
			Path:      c.cmd.Path,
			Args:      c.cmd.Args,
			StartedAt: c.startedAt,
		})
		c.mtx.RUnlock()
	}
	return &Report{
		Active: activeCommands,
	}
}

var active struct {
	mtx  sync.RWMutex
	cmds map[*Cmd]bool
}

func init() {
	active.cmds = make(map[*Cmd]bool)
}

func startPostReport(c *Cmd, err error) {
	if err != nil {
		return
	}

	active.mtx.Lock()
	prev := active.cmds[c]
	if prev {
		panic("impl error: duplicate active command")
	}
	active.cmds[c] = true
	active.mtx.Unlock()
}

func waitPostReport(c *Cmd) {
	active.mtx.Lock()
	defer active.mtx.Unlock()
	prev := active.cmds[c]
	if !prev {
		panic(fmt.Sprintf("impl error: onWaitDone must only be called on an active command: %s", c))
	}
	delete(active.cmds, c)
}
