package collector

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func Test_parseProcMounts(t *testing.T) {
	file, err := os.Open(filepath.Clean("testdata/proc/mounts.golden"))
	assert.NoError(t, err)
	defer func() { _ = file.Close() }()

	stats, err := parseProcMounts(file)
	assert.NoError(t, err)

	want := []mount{
		{device: "/dev/mapper/ssd-root", mountpoint: "/", fstype: "ext4", options: "rw,relatime,discard,errors=remount-ro"},
		{device: "/dev/sda1", mountpoint: "/boot", fstype: "ext3", options: "rw,relatime"},
		{device: "/dev/mapper/ssd-data", mountpoint: "/data", fstype: "ext4", options: "rw,relatime,discard"},
		{device: "/dev/sdc1", mountpoint: "/archive", fstype: "xfs", options: "rw,relatime"},
	}

	assert.Equal(t, want, stats)

	// test with wrong format file
	file, err = os.Open(filepath.Clean("testdata/proc/netdev.golden"))
	assert.NoError(t, err)
	defer func() { _ = file.Close() }()

	stats, err = parseProcMounts(file)
	assert.Error(t, err)
	assert.Nil(t, stats)
}

func Test_truncateDeviceName(t *testing.T) {
	var testcases = []struct {
		name string
		path string
		want string
	}{
		{name: "valid 1", path: "testdata/dev/sda", want: "sda"},
		{name: "valid 2", path: "testdata/dev/sdb2", want: "sdb2"},
		{name: "valid 3", path: "testdata/dev/mapper/ssd-root", want: "dm-1"},
		{name: "unknown", path: "testdata/dev/unknown", want: "unknown"},
	}

	for _, tc := range testcases {
		assert.Equal(t, tc.want, truncateDeviceName(tc.path))
	}
}
