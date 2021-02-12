package collector

import (
	"github.com/stretchr/testify/assert"
	"github.com/weaponry/pgscv/internal/filter"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

func TestNetdevCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"node_network_bytes_total",
			"node_network_packets_total",
			"node_network_events_total",
		},
		collector: NewNetdevCollector,
	}

	pipeline(t, input)
}

func Test_parseNetdevStats(t *testing.T) {
	file, err := os.Open(filepath.Clean("testdata/proc/netdev.golden"))
	assert.NoError(t, err)
	defer func() { _ = file.Close() }()

	f := filter.Filter{ExcludeRE: regexp.MustCompile(`docker|br-ed|virbr`)}
	stats, err := parseNetdevStats(file, f)
	assert.NoError(t, err)

	want := map[string][]float64{
		"enp2s0":          {34899781, 60249, 10, 20, 30, 40, 50, 447, 8935189, 63211, 60, 70, 80, 90, 100, 110},
		"lo":              {31433180, 57694, 15, 25, 48, 75, 71, 18, 31433180, 57694, 12, 48, 82, 38, 66, 17},
		"wlxc8be19e6279d": {68384665, 50991, 0, 85, 0, 17, 0, 44, 4138903, 29619, 1, 0, 74, 0, 4, 0},
	}

	assert.Equal(t, want, stats)
}
