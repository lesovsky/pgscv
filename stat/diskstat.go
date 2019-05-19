// Package stat is used for retrieving different kind of statistics.
// diskstat.go is related to block devices statistics which is located in /proc/diskstats.
package stat

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"strings"
)

// Diskstat is the container for storing stats per single block device
type Diskstat struct {
	/* diskstats basic */
	Major, Minor int     // 1 - major number; 2 - minor mumber
	Device       string  // 3 - device name
	Rcompleted   float64 // 4 - reads completed successfully
	Rmerged      float64 // 5 - reads merged
	Rsectors     float64 // 6 - sectors read
	Rspent       float64 // 7 - time spent reading (ms)
	Wcompleted   float64 // 8 - writes completed
	Wmerged      float64 // 9 - writes merged
	Wsectors     float64 // 10 - sectors written
	Wspent       float64 // 11 - time spent writing (ms)
	Ioinprogress float64 // 12 - I/Os currently in progress
	Tspent       float64 // 13 - time spent doing I/Os (ms)
	Tweighted    float64 // 14 - weighted time spent doing I/Os (ms)
	/* diskstats advanced */
	Uptime    float64 // system uptime, used for interval calculation
	Completed float64 // reads and writes completed
	Rawait    float64 // average time (in milliseconds) for read requests issued to the device to be served. This includes the time spent by the requests in queue and the time spent servicing them.
	Wawait    float64 // average time (in milliseconds) for write requests issued to the device to be served. This includes the time spent by the requests in queue and the time spent servicing them.
	Await     float64 // average time (in milliseconds) for I/O requests issued to the device to be served. This includes the time spent by the requests in queue and the time spent servicing them.
	Arqsz     float64 // average size (in sectors) of the requests that were issued to the device.
	Util      float64 // percentage of elapsed time during which I/O requests were issued to the device (bandwidth utilization for the device). Device saturation occurs when this value is close to 100% for devices serving requests serially.
	// But for devices serving requests in parallel, such as RAID arrays and modern SSDs, this number does not reflect their performance limits.
}

// Diskstats is the container for all vlock devices stats
type Diskstats []Diskstat

const (
	// ProcDiskstats is the file which provides IO statistics of block devices. For more details refer to Linux kernel's Documentation/iostats.txt.
	ProcDiskstats = "/proc/diskstats"
)

// ReadLocal method read stats about block devices from local 'procfs' filesystem
func (c Diskstats) ReadLocal() error {
	content, err := ioutil.ReadFile(ProcDiskstats)
	if err != nil {
		return fmt.Errorf("failed to read %s", ProcDiskstats)
	}
	reader := bufio.NewReader(bytes.NewBuffer(content))

	uptime, err := UptimeMs()
	if err != nil {
		return err
	}
	for i := 0; i < len(c); i++ {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		var ios = Diskstat{}

		_, err = fmt.Sscanln(string(line),
			&ios.Major, &ios.Minor, &ios.Device,
			&ios.Rcompleted, &ios.Rmerged, &ios.Rsectors, &ios.Rspent,
			&ios.Wcompleted, &ios.Wmerged, &ios.Wsectors, &ios.Wspent,
			&ios.Ioinprogress, &ios.Tspent, &ios.Tweighted)
		if err != nil {
			return fmt.Errorf("failed to scan data from %s", ProcDiskstats)
		}

		ios.Uptime = uptime
		c[i] = ios
	}
	return nil
}

// SingleStat method returns value of particular stat of a block device
func (d Diskstat) SingleStat(stat string) (value float64) {
	switch stat {
	case "rcompleted":
		value = d.Rcompleted
	case "rmerged":
		value = d.Rmerged
	case "rsectors":
		value = d.Rsectors
	case "rspent":
		value = d.Rspent
	case "wcompleted":
		value = d.Wspent
	case "wmerged":
		value = d.Wmerged
	case "wsectors":
		value = d.Wsectors
	case "wspent":
		value = d.Wspent
	case "ioinprogress":
		value = d.Ioinprogress
	case "tspent":
		value = d.Tspent
	case "tweighted":
		value = d.Tweighted
	case "uptime":
		value = d.Uptime
	default:
		value = 0
	}
	return value
}

// IsDeviceRotational checks kind of the attached storage, and returns true if it is rotational
func IsDeviceRotational(devpath string) (float64, error) {
	rotationalFile := devpath + "/queue/rotational"

	content, err := ioutil.ReadFile(rotationalFile)
	if err != nil {
		fmt.Printf("failed to read %s: %s", rotationalFile, err)
	}
	reader := bufio.NewReader(bytes.NewBuffer(content))
	line, _, err := reader.ReadLine()
	if err != nil {
		fmt.Printf("failed to read from buffer: %s", err)
	}

	switch string(line) {
	case "0":
		return 0, nil
	case "1":
		return 1, nil
	default:
		return -1, fmt.Errorf("unknown kind of device: %s (%s)", strings.TrimPrefix(devpath, "/sys/block/"), string(line))
	}
}

// GetDeviceScheduler returns name of the IO scheduler used by device
func GetDeviceScheduler(devpath string) (sched string, err error) {
	schedulerFile := devpath + "/queue/scheduler"

	content, err := ioutil.ReadFile(schedulerFile)
	if err != nil {
		fmt.Printf("failed to read %s: %s", schedulerFile, err)
	}
	reader := bufio.NewReader(bytes.NewBuffer(content))
	line, _, err := reader.ReadLine()
	if err != nil {
		fmt.Printf("failed to read from buffer: %s", err)
	}

	re := regexp.MustCompile(`\[[a-z-]+\]|none`)
	if sched := re.Find(line); sched != nil {
		sched := bytes.Trim(sched, "[]")
		return string(sched), nil
	}

	return "", fmt.Errorf("Failed to recognize scheduler of %s\n", strings.TrimPrefix(devpath, "/sys/block/"))
}
