package collector

import (
	"github.com/barcodepro/pgscv/internal/filter"
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
			"node_system_storage_info",
		},
		collector: NewDiskstatsCollector,
	}

	pipeline(t, input)
}

func Test_parseDiskstats(t *testing.T) {
	file, err := os.Open(filepath.Clean("testdata/proc/diskstats.golden"))
	assert.NoError(t, err)
	defer func() { _ = file.Close() }()

	f := filter.Filter{ExcludeRE: regexp.MustCompile(`^(ram|loop|fd|dm-|(h|s|v|xv)d[a-z]|nvme\d+n\d+p)\d+$`)}
	stats, err := parseDiskstats(file, f)
	assert.NoError(t, err)

	want := map[string][]float64{
		"sda": {118374, 28537, 5814772, 33586, 170999, 194921, 19277944, 181605, 0, 187400, 108536, 16519, 0, 5817512, 63312},
		"sdb": {11850, 3383, 1004986, 64473, 13797, 2051, 192184, 43282, 0, 36604, 89536, 0, 0, 0, 0},
	}

	assert.Equal(t, want, stats)
}

func Test_getStorageProperties(t *testing.T) {
	f := filter.Filter{ExcludeRE: regexp.MustCompile(`^(ram|loop|fd|(h|s|v|xv)d[a-z]|nvme\d+n\d+p)\d+$`)}

	want := []storageDeviceProperties{
		{device: "sda", rotational: "0", scheduler: "mq-deadline"},
		{device: "sdb", rotational: "1", scheduler: "deadline"},
	}

	storages, err := getStorageProperties("testdata/sys/block/*", f)
	assert.NoError(t, err)
	assert.Equal(t, want, storages)
}

func Test_getDeviceRotational(t *testing.T) {
	r, err := getDeviceRotational("testdata/sys/block/sda")
	assert.NoError(t, err)
	assert.Equal(t, "0", r)

	// Read file with bad content
	r, err = getDeviceRotational("testdata/sys/block/sdy")
	assert.Error(t, err)
	assert.Equal(t, "", r)

	// Read unknown file
	r, err = getDeviceRotational("testdata/proc/meminfo.golden")
	assert.Error(t, err)
	assert.Equal(t, "", r)
}

func Test_getDeviceScheduler(t *testing.T) {
	r, err := getDeviceScheduler("testdata/sys/block/sda")
	assert.NoError(t, err)
	assert.Equal(t, "mq-deadline", r)

	// Read file with bad content
	r, err = getDeviceScheduler("testdata/sys/block/sdz")
	assert.Error(t, err)
	assert.Equal(t, "", r)

	// Read unknown file
	r, err = getDeviceScheduler("testdata/proc/meminfo.golden")
	assert.Error(t, err)
	assert.Equal(t, "", r)
}
