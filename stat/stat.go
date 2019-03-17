// Package stat is used for retrieving different kind of statistics.
package stat

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os/exec"
	"strconv"
)

const (
	procUptime = "/proc/uptime"
)

var (
	// SysTicks stores the system timer's frequency
	SysTicks float64 = 100
)

func init() {
	cmdOutput, err := exec.Command("getconf", "CLK_TCK").Output()
	if err != nil {
		SysTicks, _ = strconv.ParseFloat(string(cmdOutput), 64)
	}
}

// Read uptime value from local procfile
func uptime() (float64, error) {
	var upsec, upcent float64

	content, err := ioutil.ReadFile(procUptime)
	if err != nil {
		return 0, fmt.Errorf("failed to read %s", procUptime)
	}

	reader := bufio.NewReader(bytes.NewBuffer(content))

	line, _, err := reader.ReadLine()
	if err != nil {
		return 0, fmt.Errorf("failed to scan data from %s", procUptime)
	}
	fmt.Sscanf(string(line), "%f.%f", &upsec, &upcent)

	return (upsec * SysTicks) + (upcent * SysTicks / 100), nil
}

// CountLinesLocal returns number of lines in the stats file
func CountLinesLocal(f string) (int, error) {
	content, err := ioutil.ReadFile(f)
	if err != nil {
		return 0, fmt.Errorf("failed to read %s", f)
	}
	r := bufio.NewReader(bytes.NewBuffer(content))

	buf := make([]byte, 128)
	count := 0
	lineSep := []byte{'\n'}

	if f == ProcNetdev {
		count = count - 2 // Shift the counter because '/proc/net/dev' contains 2 lines of header
	}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil
		case err != nil:
			return count, fmt.Errorf("failed to count rows: %s", err)
		}
	}
}
