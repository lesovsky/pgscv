package collector

import (
	"testing"
)

func TestDiskstatsCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"node_disk_reads_completed_total",
			"node_disk_reads_merged_total",
			"node_disk_read_bytes_total",
			"node_disk_read_time_seconds_total",
			"node_disk_writes_completed_total",
			"node_disk_writes_merged_total",
			"node_disk_written_bytes_total",
			"node_disk_write_time_seconds_total",
			"node_disk_io_now",
			"node_disk_io_time_seconds_total",
			"node_disk_io_time_weighted_seconds_total",
		},
		optional: []string{
			// since linux kernel 4.18+
			"node_disk_discards_completed_total",
			"node_disk_discards_merged_total",
			"node_disk_discarded_sectors_total",
			"node_disk_discard_time_seconds_total",
			// since linux kernel 5.5+
			"node_disk_flush_requests_total",
			"node_disk_flush_requests_time_seconds_total",
		},
		collector: NewDiskstatsCollector,
	}

	pipeline(t, input)
}
