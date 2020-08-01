package collector

import (
	"github.com/stretchr/testify/assert"
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

func Test_getNetdevStats(t *testing.T) {
	file, err := os.Open(filepath.Clean("testdata/procnetdev.golden"))
	assert.NoError(t, err)
	defer func() { _ = file.Close() }()

	re := regexp.MustCompile(`docker|br-ed|virbr`)
	stats, err := parseNetdevStats(file, re)
	assert.NoError(t, err)

	want := []netdevStat{
		{device: "enp2s0",
			rbytes: 34899781, rpackets: 60249, rerrs: 10, rdrop: 20, rfifo: 30, rframe: 40, rcompressed: 50, rmulticast: 447,
			tbytes: 8935189, tpackets: 63211, terrs: 60, tdrop: 70, tfifo: 80, tcolls: 90, tcarrier: 100, tcompressed: 110,
		},
		{device: "lo",
			rbytes: 31433180, rpackets: 57694, rerrs: 15, rdrop: 25, rfifo: 48, rframe: 75, rcompressed: 71, rmulticast: 18,
			tbytes: 31433180, tpackets: 57694, terrs: 12, tdrop: 48, tfifo: 82, tcolls: 38, tcarrier: 66, tcompressed: 17,
		},
		{device: "wlxc8be19e6279d",
			rbytes: 68384665, rpackets: 50991, rerrs: 0, rdrop: 85, rfifo: 0, rframe: 17, rcompressed: 0, rmulticast: 44,
			tbytes: 4138903, tpackets: 29619, terrs: 1, tdrop: 0, tfifo: 74, tcolls: 0, tcarrier: 4, tcompressed: 0,
		},
	}

	assert.Equal(t, want, stats)
}
