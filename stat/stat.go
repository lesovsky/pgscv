package stat

import (
	"io/ioutil"
	"fmt"
	"bufio"
	"bytes"
	"io"
	"os/exec"
	"strconv"
)

const (
	PROC_UPTIME = "/proc/uptime"
)

var (
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

	content, err := ioutil.ReadFile(PROC_UPTIME)
	if err != nil {
		return 0, fmt.Errorf("failed to read %s", PROC_UPTIME)
	}

	reader := bufio.NewReader(bytes.NewBuffer(content))

	line, _, err := reader.ReadLine()
	if err != nil {
		return 0, fmt.Errorf("failed to scan data from %s", PROC_UPTIME)
	}
	fmt.Sscanf(string(line), "%f.%f", &upsec, &upcent)

	return (upsec * SysTicks) + (upcent * SysTicks / 100), nil
}

// Count lines in local file
func CountLinesLocal(f string) (int, error) {
	content, err := ioutil.ReadFile(f)
	if err != nil {
		return 0, fmt.Errorf("failed to read %s", f)
	}
	r := bufio.NewReader(bytes.NewBuffer(content))

	buf := make([]byte, 128)
	count := 0
	lineSep := []byte{'\n'}

	if f == PROC_NETDEV {
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