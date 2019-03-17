// Package stat is used for retrieving different kind of statistics.
// memstat.go is related to memory/swap usage stats which is located in '/proc/meminfo'
package stat

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	procMeminfo          = "/proc/meminfo"
	sysfsNumaNodePattern = "/sys/devices/system/node/node*"
)

// Meminfo is the container for memory/swap usage stats
type Meminfo struct {
	MemTotal       uint64
	MemFree        uint64
	MemUsed        uint64
	SwapTotal      uint64
	SwapFree       uint64
	SwapUsed       uint64
	MemCached      uint64
	MemBuffers     uint64
	MemAvailable   uint64
	MemDirty       uint64
	MemWriteback   uint64
	MemSlab        uint64
	HugePagesTotal uint64
	HugePagesFree  uint64
	HugePagesRsvd  uint64
	HugePagesSurp  uint64
	HugePageSz     uint64
}

// ReadLocal method reads stats about memory/swap from local 'procfs' filesystem
func (m *Meminfo) ReadLocal() {
	content, err := ioutil.ReadFile(procMeminfo)
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
			// do kB -> bytes conversion if necessary
			if len(fields) == 3 {
				value *= 1024
			}

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
			case "MemAvailable:":
				m.MemAvailable = value
			case "Slab:":
				m.MemSlab = value
			case "HugePages_Total:":
				m.HugePagesTotal = value
			case "HugePages_Free:":
				m.HugePagesFree = value
			case "HugePages_Rsvd:":
				m.HugePagesRsvd = value
			case "HugePages_Surp:":
				m.HugePagesSurp = value
			case "Hugepagesize:":
				m.HugePageSz = value
			}
		}
	}
	m.MemUsed = m.MemTotal - m.MemFree - m.MemCached - m.MemBuffers - m.MemSlab
	m.SwapUsed = m.SwapTotal - m.SwapFree
	m.HugePagesTotal = m.HugePagesTotal * m.HugePageSz
	m.HugePagesFree = m.HugePagesFree * m.HugePageSz
	m.HugePagesRsvd = m.HugePagesRsvd * m.HugePageSz
	m.HugePagesSurp = m.HugePagesSurp * m.HugePageSz
}

// SingleStat method returns value of particular memory/swap stats
func (m Meminfo) SingleStat(stat string) (value uint64) {
	switch stat {
	case "mem_total":
		value = m.MemTotal
	case "mem_free":
		value = m.MemFree
	case "mem_used":
		value = m.MemUsed
	case "swap_total":
		value = m.SwapTotal
	case "swap_free":
		value = m.SwapFree
	case "swap_used":
		value = m.SwapUsed
	case "mem_cached":
		value = m.MemCached
	case "mem_dirty":
		value = m.MemDirty
	case "mem_writeback":
		value = m.MemWriteback
	case "mem_buffers":
		value = m.MemBuffers
	case "mem_available":
		value = m.MemAvailable
	case "mem_slab":
		value = m.MemSlab
	case "hp_total":
		value = m.HugePagesTotal
	case "hp_free":
		value = m.HugePagesFree
	case "hp_rsvd":
		value = m.HugePagesRsvd
	case "hp_surp":
		value = m.HugePagesSurp
	case "hp_pagesize":
		value = m.HugePageSz
	default:
		value = 0
	}
	return value
}

// CountNumaNodes returns number of NUMA nodes in the system
func CountNumaNodes() (n int, err error) {
	d, err := filepath.Glob(sysfsNumaNodePattern)
	if err != nil {
		return 0, fmt.Errorf("failed counting NUMA nodes, malformed pattern: %s", err)
	}
	return len(d), nil
}
