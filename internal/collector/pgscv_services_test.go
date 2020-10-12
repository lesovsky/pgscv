package collector

import "testing"

func TestPgscvServicesCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"pgscv_services_registered_total",
		},
		collector: NewPgscvServicesCollector,
	}

	pipeline(t, input)
}
