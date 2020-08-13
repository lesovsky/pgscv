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
			"node_cpu_guest_seconds_total",
		},
		collector: NewCPUCollector,
	}

	pipeline(t, input)
}

func Test_parseProcCPUStat(t *testing.T) {
	file, err := os.Open(filepath.Clean("testdata/proc/stat.golden"))
	assert.NoError(t, err)
	defer func() { _ = file.Close() }()

	want := cpuStat{
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
	}

	// assume that sys_ticks is 100
	got, err := parseProcCPUStat(file, 100)
	assert.NoError(t, err)

	assert.Equal(t, want, got)
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