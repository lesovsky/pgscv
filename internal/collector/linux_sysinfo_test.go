package collector

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestSysInfoCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"node_platform_info", "node_os_info",
		},
		optional:  []string{},
		collector: NewSysInfoCollector,
	}

	pipeline(t, input)
}

func Test_getSysInfo(t *testing.T) {
	info, err := getSysInfo()
	assert.NoError(t, err)
	assert.NotNil(t, info)
}

func Test_parseOsRelease(t *testing.T) {
	file, err := os.Open(filepath.Clean("testdata/etc/os-release.golden"))
	assert.NoError(t, err)
	defer func() { _ = file.Close() }()

	name, version, err := parseOsRelease(file)
	assert.NoError(t, err)
	assert.NotEqual(t, "", name)
	assert.NotEqual(t, "", version)
}
