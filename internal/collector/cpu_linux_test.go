package collector

import (
	"testing"
)

func TestCPUCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"node_cpu_seconds_total",
			"node_cpu_guest_seconds_total",
		},
		collector: NewCPUCollector,
	}

	pipeline(t, input)
}
