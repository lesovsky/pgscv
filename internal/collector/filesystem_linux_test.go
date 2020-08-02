package collector

import (
	"context"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"
)

func TestFilesystemCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"node_filesystem_bytes",
			"node_filesystem_files",
		},
		collector: NewFilesystemCollector,
	}

	pipeline(t, input)
}

func Test_parseProcMounts(t *testing.T) {
	file, err := os.Open(filepath.Clean("testdata/procmounts.golden"))
	assert.NoError(t, err)
	defer func() { _ = file.Close() }()

	filter := regexp.MustCompile(`^(ext3|ext4|xfs|btrfs)$`)

	stats, err := parseProcMounts(file, filter)
	assert.NoError(t, err)

	want := []filesystemStat{
		{device: "/dev/mapper/ssd-root", mountpoint: "/", fstype: "ext4", options: "rw,relatime,discard,errors=remount-ro"},
		{device: "/dev/sda1", mountpoint: "/boot", fstype: "ext3", options: "rw,relatime"},
		{device: "/dev/mapper/ssd-data", mountpoint: "/data", fstype: "ext4", options: "rw,relatime,discard"},
		{device: "/dev/sdc1", mountpoint: "/archive", fstype: "xfs", options: "rw,relatime"},
	}

	assert.Equal(t, want, stats)

	// test with wrong format file
	file, err = os.Open(filepath.Clean("testdata/procnetdev.golden"))
	assert.NoError(t, err)
	defer func() { _ = file.Close() }()

	stats, err = parseProcMounts(file, nil)
	assert.Error(t, err)
	assert.Nil(t, stats)
}

func Test_parseFilesystemStats(t *testing.T) {
	file, err := os.Open(filepath.Clean("/proc/mounts"))
	assert.NoError(t, err)

	stats, err := parseFilesystemStats(file, regexp.MustCompile(`^tmpfs`))
	assert.NoError(t, err)
	assert.Greater(t, len(stats), 1)
	assert.Greater(t, stats[0].size, float64(0))
	assert.Greater(t, stats[0].free, float64(0))
	assert.Greater(t, stats[0].avail, float64(0))
	assert.Greater(t, stats[0].files, float64(0))
	assert.Greater(t, stats[0].filesfree, float64(0))

	_ = file.Close()

	// test with wrong format file
	file, err = os.Open(filepath.Clean("testdata/procnetdev.golden"))
	assert.NoError(t, err)

	stats, err = parseProcMounts(file, nil)
	assert.Error(t, err)
	assert.Nil(t, stats)
	_ = file.Close()
}

func Test_readMountpointStat(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	ch := make(chan filesystemStat)
	go readMountpointStat("/", ch)

	select {
	case response := <-ch:
		assert.Greater(t, response.size, float64(0))
		assert.Greater(t, response.free, float64(0))
		assert.Greater(t, response.avail, float64(0))
		assert.Greater(t, response.files, float64(0))
		assert.Greater(t, response.filesfree, float64(0))
	case <-ctx.Done():
		assert.Fail(t, "context cancelled: ", ctx.Err())
	}

}
