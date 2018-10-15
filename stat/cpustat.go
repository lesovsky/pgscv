package stat

import (
	"io/ioutil"
	"bufio"
	"bytes"
	"io"
	"strings"
	"fmt"
)

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
	PROC_STAT          = "/proc/stat"
)

/* Read CPU usage raw values from statfile and save to pre-calculation struct */
func (s *CpuRawstat) ReadLocal() {
	content, err := ioutil.ReadFile(PROC_STAT)
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

// Function return number of ticks for particular mode
func (s *CpuRawstat) SingleStat(mode string) (ticks float64) {
	switch mode {
	case "user": ticks = s.User
	case "nice": ticks = s.Nice
	case "system": ticks = s.Sys
	case "idle": ticks = s.Idle
	case "iowait": ticks = s.Iowait
	case "irq": ticks = s.Irq
	case "softirq": ticks = s.Softirq
	case "steal": ticks = s.Steal
	case "guest": ticks = s.Guest
	case "guest_nice": ticks = s.GstNice
	case "total": ticks = s.Total
	default: ticks = 0
	}
	return ticks
}