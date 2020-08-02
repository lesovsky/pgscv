package collector

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSystemCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"node_system_sysctl",
		},
		collector: NewSystemCollector,
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
