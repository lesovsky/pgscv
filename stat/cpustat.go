// Package stat is used for retrieving different kind of statistics.
// cpustat.go is related to CPU usage stats which is located in '/proc/stat'
package stat

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// CpuRawstat is a container for raw values collected from cpu-stat source
type CpuRawstat struct {
	Entry   string
	User    float64
	Nice    float64
	Sys     float64
	Idle    float64
	Iowait  float64
	Irq     float64
	Softirq float64
	Steal   float64
	Guest   float64
	GstNice float64
	Total   float64
}

const (
	procStatFile    = "/proc/stat"
	sysfsCpuPattern = "/sys/devices/system/cpu/cpu*"
)

// ReadLocal reads CPU raw stats from local 'procfs' filesystem
func (s *CpuRawstat) ReadLocal() {
	content, err := ioutil.ReadFile(procStatFile)
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
			if fields[0] == "cpu" {
				_, err = fmt.Sscanln(string(line),
					&s.Entry, &s.User, &s.Nice, &s.Sys, &s.Idle,
					&s.Iowait, &s.Irq, &s.Softirq, &s.Steal, &s.Guest, &s.GstNice)
				if err != nil {
					return
				}

				/* Use total instead of uptime, because of separate reading of /proc/uptime and /proc/stat leads to stat's skew */
				s.Total = s.User + s.Nice + s.Sys + s.Idle + s.Iowait + s.Irq + s.Softirq + s.Steal + s.Guest
			}
		}
	}
	return
}

// SingleStat returns number of ticks for particular mode
func (s *CpuRawstat) SingleStat(mode string) (ticks float64) {
	switch mode {
	case "user":
		ticks = s.User
	case "nice":
		ticks = s.Nice
	case "system":
		ticks = s.Sys
	case "idle":
		ticks = s.Idle
	case "iowait":
		ticks = s.Iowait
	case "irq":
		ticks = s.Irq
	case "softirq":
		ticks = s.Softirq
	case "steal":
		ticks = s.Steal
	case "guest":
		ticks = s.Guest
	case "guest_nice":
		ticks = s.GstNice
	case "total":
		ticks = s.Total
	default:
		ticks = 0
	}
	return ticks
}

// CountCpu returns number of online and offline CPU cores
func CountCpu() (online, offline int, err error) {
	var onlineCnt, offlineCnt int

	dirs, err := filepath.Glob(sysfsCpuPattern)
	if err != nil {
		return 0, 0, fmt.Errorf("failed counting CPUs, malformed pattern: %s", err)
	}

	for _, d := range dirs {
		if strings.HasSuffix(d, "/cpu0") { // cpu0 has no 'online' file and always online, just increment counter
			onlineCnt++
			continue
		}
		re := regexp.MustCompile(`cpu[0-9]+$`)
		file := d + "/online"
		if re.MatchString(d) {
			content, err := ioutil.ReadFile(file)
			if err != nil {
				return 0, 0, fmt.Errorf("failed to read %s: %s", file, err)
			}
			reader := bufio.NewReader(bytes.NewBuffer(content))
			line, _, err := reader.ReadLine()
			if err != nil {
				return 0, 0, fmt.Errorf("failed to read from buffer: %s", err)
			}

			switch string(line) {
			case "0":
				offlineCnt++
			case "1":
				onlineCnt++
			default:
				fmt.Printf("failed counting CPUs, unknown value in %s: %s", file, line)
			}
		}
	}
	return onlineCnt, offlineCnt, nil
}

// CountScalingGovernors returns map with scaling governors and number of cores that use specific governor
func CountScalingGovernors() (g map[string]int, err error) {
	g = make(map[string]int)
	dirs, err := filepath.Glob(sysfsCpuPattern)
	if err != nil {
		return nil, fmt.Errorf("failed couning CPUs governors, malformed pattern: %s", err)
	}

	for _, d := range dirs {
		re := regexp.MustCompile(`cpu[0-9]+$`)
		if !re.MatchString(d) { // skip other than 'cpu*' dirs
			continue
		}
		fi, err := os.Stat(d + "/cpufreq")
		if err != nil {
			continue // cpufreq dir not found -- no cpu scaling used
		}
		file := d + "/cpufreq" + "/scaling_governor"
		if fi.IsDir() {
			content, err := ioutil.ReadFile(file)
			if err != nil {
				return nil, fmt.Errorf("failed to read %s: %s", file, err)
			}
			reader := bufio.NewReader(bytes.NewBuffer(content))
			line, _, err := reader.ReadLine()
			if err != nil {
				return nil, fmt.Errorf("failed to read from buffer: %s", err)
			}
			g[string(line)]++
		}
	}
	return g, nil
}
