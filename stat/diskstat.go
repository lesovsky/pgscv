// Stuff related to diskstats which is located at /proc/diskstats.

package stat

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
)

// Used for storing stats per single device
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

// Container for all stats from proc-file
type Diskstats []Diskstat

const (
	// The file provides IO statistics of block devices. For more details refer to Linux kernel's Documentation/iostats.txt.
	PROC_DISKSTATS       = "/proc/diskstats"
)

// Read stats from local procfs source
func (c Diskstats) ReadLocal() error {
	content, err := ioutil.ReadFile(PROC_DISKSTATS)
	if err != nil {
		return fmt.Errorf("failed to read %s", PROC_DISKSTATS)
	}
	reader := bufio.NewReader(bytes.NewBuffer(content))

	uptime, err := uptime()
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
			return fmt.Errorf("failed to scan data from %s", PROC_DISKSTATS)
		}

		ios.Uptime = uptime
		c[i] = ios
	}

	return nil
}

// Function returns value of particular stat of a block device
func (d Diskstat) SingleStat(stat string) (value float64) {
	switch stat {
	case "rcompleted": value = d.Rcompleted
	case "rmerged": value = d.Rmerged
	case "rsectors": value = d.Rsectors
	case "rspent": value = d.Rspent
	case "wcompleted": value = d.Wspent
	case "wmerged": value = d.Wmerged
	case "wsectors": value = d.Wsectors
	case "wspent": value = d.Wspent
	case "ioinprogress": value = d.Ioinprogress
	case "tspent": value = d.Tspent
	case "tweighted": value = d.Tweighted
	case "uptime": value = d.Uptime
	default: value = 0
	}
	return value
}
