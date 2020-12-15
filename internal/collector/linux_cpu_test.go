package collector

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestCPUCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"node_cpu_seconds_total",
			"node_cpu_seconds_all_total",
			"node_cpu_guest_seconds_total",
			"node_uptime_up_seconds_total",
			"node_uptime_idle_seconds_total",
		},
		collector: NewCPUCollector,
	}

	pipeline(t, input)
}

func Test_parseProcCPUStat(t *testing.T) {
	testcases := []struct {
		in    string
		valid bool
		want  cpuStat
	}{
		{in: "testdata/proc/stat.golden", valid: true, want: cpuStat{
			user:      30976.68,
			nice:      15.93,
			system:    14196.18,
			idle:      1322422.58,
			iowait:    425.35,
			irq:       0,
			softirq:   3846.86,
			steal:     0,
			guest:     0,
			guestnice: 0,
		}},
		{in: "testdata/proc/stat.invalid", valid: false},
	}

	for _, tc := range testcases {
		file, err := os.Open(filepath.Clean(tc.in))
		assert.NoError(t, err)

		got, err := parseProcCPUStat(file, 100)
		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		} else {
			assert.Error(t, err)
		}
		assert.NoError(t, file.Close())
	}
}

func Test_parseCPUStat(t *testing.T) {
	var testcases = []struct {
		valid bool
		line  string
		want  cpuStat
	}{
		{
			valid: true,
			line:  "cpu 3097668 1593 1419618 132242258 42535 0 384686 0 0 0",
			want: cpuStat{
				user: 30976.68, nice: 15.93, system: 14196.18, idle: 1322422.58, iowait: 425.35,
				irq: 0, softirq: 3846.86, steal: 0, guest: 0, guestnice: 0,
			},
		},
		{valid: false, line: "invalid 3097668 1593 1419618 132242258 42535 0 384686"},
		{valid: false, line: "invalid invalid"},
	}

	// assume that sys_ticks is 100
	for _, tc := range testcases {
		got, err := parseCPUStat(tc.line, 100)
		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		} else {
			assert.Error(t, err)
		}
	}
}

func Test_getProcUptime(t *testing.T) {
	up, idle, err := getProcUptime("testdata/proc/uptime.golden")
	assert.NoError(t, err)
	assert.Equal(t, float64(187477.470), up)
	assert.Equal(t, float64(1397296.120), idle)

	_, _, err = getProcUptime("testdata/proc/stat.golden")
	assert.Error(t, err)
}
