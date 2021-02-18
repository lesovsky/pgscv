package collector

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestLoadAverageCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"node_load1",
			"node_load5",
			"node_load15",
		},
		collector: NewLoadAverageCollector,
	}

	pipeline(t, input)
}

func Test_getLoadAverageStats(t *testing.T) {
	loads, err := getLoadAverageStats()
	assert.NoError(t, err)
	assert.Len(t, loads, 3)
}

func Test_parseLoadAverageStats(t *testing.T) {
	data, err := os.ReadFile("./testdata/proc/loadavg.golden")
	assert.NoError(t, err)

	loads, err := parseLoadAverageStats(string(data))
	assert.NoError(t, err)
	assert.Equal(t, 1.15, loads[0])
	assert.Equal(t, 1.36, loads[1])
	assert.Equal(t, 1.24, loads[2])

	_, err = parseLoadAverageStats("invalid data")
	assert.Error(t, err)

	_, err = parseLoadAverageStats("1 qq 2 1/123 12312")
	assert.Error(t, err)
}
