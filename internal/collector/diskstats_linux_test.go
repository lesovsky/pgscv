package collector

import (
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"regexp"
	"testing"
)

func TestDiskstatsCollector_Update(t *testing.T) {
	var requiredMetricNames = []string{
		"pgscv_disk_reads_completed_total",
		"pgscv_disk_reads_merged_total",
		"pgscv_disk_read_bytes_total",
		"pgscv_disk_read_time_seconds_total",
		"pgscv_disk_writes_completed_total",
		"pgscv_disk_writes_merged_total",
		"pgscv_disk_written_bytes_total",
		"pgscv_disk_write_time_seconds_total",
		"pgscv_disk_io_now",
		"pgscv_disk_io_time_seconds_total",
		"pgscv_disk_io_time_weighted_seconds_total",
		"pgscv_disk_discards_completed_total",
		"pgscv_disk_discards_merged_total",
		"pgscv_disk_discarded_sectors_total",
		"pgscv_disk_discard_time_seconds_total",
		"pgscv_disk_flush_requests_total",
		"pgscv_disk_flush_requests_time_seconds_total",
	}

	collector, err := NewDiskstatsCollector(prometheus.Labels{"example_label": "example_value"})
	assert.NoError(t, err)
	ch := make(chan prometheus.Metric)

	go func() {
		err := collector.Update(Config{ServiceType: model.ServiceTypeSystem}, ch)
		assert.NoError(t, err)
		close(ch)
	}()

	// receive metrics from channel, extract name from the metric and check name of received metric exists in the test slice
	for metric := range ch {
		re := regexp.MustCompile(`fqName: "([a-z_]+)"`)
		match := re.FindStringSubmatch(metric.Desc().String())[1]
		assert.Contains(t, requiredMetricNames, match)
	}
}
