package collector

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

func TestDiskstatsCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"node_disk_completed_total",
			"node_disk_merged_total",
			"node_disk_bytes_total",
			"node_disk_time_seconds_total",
			"node_disk_io_now",
			"node_disk_io_time_seconds_total",
			"node_disk_io_time_weighted_seconds_total",
		},
		collector: NewDiskstatsCollector,
	}

	pipeline(t, input)
}

func Test_parseDiskstats(t *testing.T) {
	file, err := os.Open(filepath.Clean("testdata/procdiskstats.golden"))
	assert.NoError(t, err)
	defer func() { _ = file.Close() }()

	re := regexp.MustCompile("^(ram|loop|fd|dm-|(h|s|v|xv)d[a-z]|nvme\\d+n\\d+p)\\d+$")
	stats, err := parseDiskstats(file, re)
	assert.NoError(t, err)

	want := map[string][]float64{
		"sda": {118374, 28537, 5814772, 33586, 170999, 194921, 19277944, 181605, 0, 187400, 108536, 16519, 0, 5817512, 63312},
		"sdb": {11850, 3383, 1004986, 64473, 13797, 2051, 192184, 43282, 0, 36604, 89536, 0, 0, 0, 0},
	}

	assert.Equal(t, want, stats)
}
