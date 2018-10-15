// Stuff related to memory/swap usage stats

package stat

import (
	"bufio"
	"bytes"
		"io"
	"io/ioutil"
	"strconv"
	"strings"
)

const (
	PROC_MEMINFO       = "/proc/meminfo"
)

// Conatiner for memory/swap usage stats
type Meminfo struct {
	MemTotal     uint64
	MemFree      uint64
	MemUsed      uint64
	SwapTotal    uint64
	SwapFree     uint64
	SwapUsed     uint64
	MemCached    uint64
	MemBuffers   uint64
	MemDirty     uint64
	MemWriteback uint64
	MemSlab      uint64
}

// Read stats from local procfile source
func (m *Meminfo) ReadLocal() {
	content, err := ioutil.ReadFile(PROC_MEMINFO)
	if err != nil {
		return
	}

	reader := bufio.NewReader(bytes.NewBuffer(content))
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}

		fields := strings.Fields(string(line))
		if len(fields) > 0 {
			value, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return
			}
			value *= 1024 /* kB -> bytes conversion */

			switch fields[0] {
			case "MemTotal:":
				m.MemTotal = value
			case "MemFree:":
				m.MemFree = value
			case "SwapTotal:":
				m.SwapTotal = value
			case "SwapFree:":
				m.SwapFree = value
			case "Cached:":
				m.MemCached = value
			case "Dirty:":
				m.MemDirty = value
			case "Writeback:":
				m.MemWriteback = value
			case "Buffers:":
				m.MemBuffers = value
			case "Slab:":
				m.MemSlab = value
			}
		}
	}
	m.MemUsed = m.MemTotal - m.MemFree - m.MemCached - m.MemBuffers - m.MemSlab
	m.SwapUsed = m.SwapTotal - m.SwapFree
}

// Function returns value of particular memory/swap stat
func (m Meminfo) SingleStat(stat string) (value uint64) {
	switch stat {
	case "mem_total": value = m.MemTotal
	case "mem_free": value = m.MemFree
	case "mem_used": value = m.MemUsed
	case "swap_total": value = m.SwapTotal
	case "swap_free": value = m.SwapFree
	case "swap_used": value = m.SwapUsed
	case "mem_cached": value = m.MemCached
	case "mem_dirty": value = m.MemDirty
	case "mem_writeback": value = m.MemWriteback
	case "mem_buffers": value = m.MemBuffers
	case "mem_slab": value = m.MemSlab
	default: value = 0
	}
	return value
}