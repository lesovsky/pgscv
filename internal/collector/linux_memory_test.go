package collector

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestMeminfoCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"node_memory_meminfo",
		},
		collector: NewMeminfoCollector,
	}

	pipeline(t, input)
}

func Test_parseMeminfoStats(t *testing.T) {
	file, err := os.Open(filepath.Clean("testdata/proc/meminfo.golden"))
	assert.NoError(t, err)
	defer func() { _ = file.Close() }()

	stats, err := parseMeminfoStats(file)
	assert.NoError(t, err)

	want := map[string]float64{
		"MemTotal":          32839484 * 1024,
		"MemFree":           21570088 * 1024,
		"MemAvailable":      26190600 * 1024,
		"Buffers":           604064 * 1024,
		"Cached":            4361844 * 1024,
		"SwapCached":        0 * 1024,
		"Active":            7785324 * 1024,
		"Inactive":          2591484 * 1024,
		"Active(anon)":      5448748 * 1024,
		"Inactive(anon)":    344784 * 1024,
		"Active(file)":      2336576 * 1024,
		"Inactive(file)":    2246700 * 1024,
		"Unevictable":       0 * 1024,
		"Mlocked":           0 * 1024,
		"SwapTotal":         16777212 * 1024,
		"SwapFree":          16777212 * 1024,
		"Dirty":             36404 * 1024,
		"Writeback":         0 * 1024,
		"AnonPages":         5410948 * 1024,
		"Mapped":            1197820 * 1024,
		"Shmem":             386884 * 1024,
		"KReclaimable":      502080 * 1024,
		"Slab":              692516 * 1024,
		"SReclaimable":      502080 * 1024,
		"SUnreclaim":        190436 * 1024,
		"KernelStack":       16848 * 1024,
		"PageTables":        54472 * 1024,
		"NFS_Unstable":      0 * 1024,
		"Bounce":            0 * 1024,
		"WritebackTmp":      0 * 1024,
		"CommitLimit":       33196952 * 1024,
		"Committed_AS":      12808144 * 1024,
		"VmallocTotal":      34359738367 * 1024,
		"VmallocUsed":       34976 * 1024,
		"VmallocChunk":      0 * 1024,
		"Percpu":            6528 * 1024,
		"HardwareCorrupted": 0 * 1024,
		"AnonHugePages":     0 * 1024,
		"ShmemHugePages":    0 * 1024,
		"ShmemPmdMapped":    0 * 1024,
		"FileHugePages":     0 * 1024,
		"FilePmdMapped":     0 * 1024,
		"CmaTotal":          0 * 1024,
		"CmaFree":           0 * 1024,
		"HugePages_Total":   0 * 2048 * 1024,
		"HugePages_Free":    0 * 2048 * 1024,
		"HugePages_Rsvd":    0 * 2048 * 1024,
		"HugePages_Surp":    0 * 2048 * 1024,
		"Hugepagesize":      2048 * 1024,
		"Hugetlb":           0 * 1024,
		"DirectMap4k":       482128 * 1024,
		"DirectMap2M":       13101056 * 1024,
		"DirectMap1G":       19922944 * 1024,
	}

	assert.Equal(t, want, stats)

	// test with wrong format file
	file, err = os.Open(filepath.Clean("testdata/proc/netdev.golden"))
	assert.NoError(t, err)
	defer func() { _ = file.Close() }()

	stats, err = parseMeminfoStats(file)
	assert.Error(t, err)
	assert.Nil(t, stats)
}
