package collector

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestSystemCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"node_system_sysctl",
			"node_system_cpu_cores_total",
			"node_system_numa_nodes_total",
			"node_context_switches_total",
			"node_forks_total",
			"node_boot_time_seconds",
		},
		optional: []string{
			"node_system_scaling_governors_total",
		},
		collector: NewSysconfigCollector,
	}

	pipeline(t, input)
}

func Test_readSysctls(t *testing.T) {
	var list = []string{"vm.dirty_ratio", "vm.dirty_background_ratio", "vm.dirty_expire_centisecs", "vm.dirty_writeback_centisecs"}

	sysctls := readSysctls(list)
	assert.NotNil(t, sysctls)
	assert.Len(t, sysctls, 4)

	for _, s := range list {
		if _, ok := sysctls[s]; !ok {
			assert.Fail(t, "sysctl not found in the list")
			continue
		}
		assert.Greater(t, sysctls[s], float64(0))
	}

	// unknown sysctl
	res := readSysctls([]string{"invalid"})
	assert.Len(t, res, 0)

	// non-float64 sysctl
	res = readSysctls([]string{"kernel.version"})
	assert.Len(t, res, 0)
}

func Test_countCPUCores(t *testing.T) {
	online, offline, err := countCPUCores("testdata/sys/devices.system/cpu/cpu*")
	assert.NoError(t, err)
	assert.Equal(t, float64(2), online)
	assert.Equal(t, float64(1), offline)
}

func Test_countScalingGovernors(t *testing.T) {
	want := map[string]float64{
		"powersave":   2,
		"performance": 2,
	}

	governors, err := countScalingGovernors("testdata/sys/devices.system/cpu/cpu*")
	assert.NoError(t, err)
	assert.Equal(t, want, governors)
}

func Test_countNumaNodes(t *testing.T) {
	n, err := countNumaNodes("testdata/sys/devices.system/node/node*")
	assert.NoError(t, err)
	assert.Equal(t, float64(2), n)
}

func Test_parseProcStat(t *testing.T) {
	file, err := os.Open(filepath.Clean("testdata/proc/stat.golden"))
	assert.NoError(t, err)
	defer func() { _ = file.Close() }()

	want := systemProcStat{
		ctxt:  3253088019,
		btime: 1596255715,
		forks: 214670,
	}

	got, err := parseProcStat(file)
	assert.NoError(t, err)
	assert.Equal(t, want, got)

	// open invalid file
	file, err = os.Open(filepath.Clean("testdata/proc/stat.invalid.golden"))
	assert.NoError(t, err)
	_, err = parseProcStat(file)
	assert.Error(t, err)

}
