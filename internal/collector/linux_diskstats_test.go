package collector

import (
	"github.com/stretchr/testify/assert"
	"github.com/weaponry/pgscv/internal/filter"
	"github.com/weaponry/pgscv/internal/model"
	"os"
	"path/filepath"
	"testing"
)

func TestDiskstatsCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"node_disk_completed_total",
			"node_disk_completed_all_total",
			"node_disk_merged_total",
			"node_disk_merged_all_total",
			"node_disk_bytes_total",
			"node_disk_bytes_all_total",
			"node_disk_time_seconds_total",
			"node_disk_time_seconds_all_total",
			"node_disk_io_now",
			"node_disk_io_time_seconds_total",
			"node_disk_io_time_weighted_seconds_total",
			"node_system_storage_info",
			"node_system_storage_size_bytes",
		},
		collector:         NewDiskstatsCollector,
		collectorSettings: model.CollectorSettings{Filters: filter.New()},
	}

	pipeline(t, input)
}

func Test_parseDiskstats(t *testing.T) {
	file, err := os.Open(filepath.Clean("testdata/proc/diskstats.golden"))
	assert.NoError(t, err)
	defer func() { _ = file.Close() }()

	stats, err := parseDiskstats(file)
	assert.NoError(t, err)

	want := map[string][]float64{
		"sda": {118374, 28537, 5814772, 33586, 170999, 194921, 19277944, 181605, 0, 187400, 108536, 16519, 0, 5817512, 63312},
		"sdb": {11850, 3383, 1004986, 64473, 13797, 2051, 192184, 43282, 0, 36604, 89536, 0, 0, 0, 0},
	}

	assert.Equal(t, want, stats)
}

func Test_getStorageProperties(t *testing.T) {
	want := []storageDeviceProperties{
		{device: "sda", rotational: "0", scheduler: "mq-deadline", size: 234441648, virtual: "true"},
		{device: "sdb", rotational: "1", scheduler: "deadline", size: 3907029168, virtual: "false", model: "TEST HARDDISK WITH LONG LONG LON"},
	}

	storages, err := getStorageProperties("testdata/sys/block/*")
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

func Test_getDeviceSize(t *testing.T) {
	sz, err := getDeviceSize("testdata/sys/block/sda")
	assert.NoError(t, err)
	assert.Equal(t, int64(234441648), sz)

	// Read file with bad content
	sz, err = getDeviceSize("testdata/sys/block/sdz")
	assert.Error(t, err)
	assert.Equal(t, int64(0), sz)

	// Read unknown file
	sz, err = getDeviceSize("testdata/proc/meminfo.golden")
	assert.Error(t, err)
	assert.Equal(t, int64(0), sz)
}

func Test_getDeviceModel(t *testing.T) {
	m, err := getDeviceModel("testdata/sys/block/sdb")
	assert.NoError(t, err)
	assert.Equal(t, "TEST HARDDISK WITH LONG LONG LON", m)

	// Read file with bad content
	m, err = getDeviceModel("testdata/sys/block/sdz")
	assert.Error(t, err)
	assert.Equal(t, "", m)

	// Read unknown file
	m, err = getDeviceModel("testdata/proc/meminfo.golden")
	assert.Error(t, err)
	assert.Equal(t, "", m)
}
