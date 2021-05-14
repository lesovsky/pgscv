package collector

import (
	"github.com/weaponry/pgscv/internal/model"
	"testing"
)

func TestPostgresCustomCollector_Update(t *testing.T) {
	settings := model.CollectorSettings{
		Subsystems: map[string]model.MetricsSubsystem{
			"example1": {
				Databases: "pgscv_fixtures",
				Query:     "SELECT 'label1' as l1, 1 as v1",
				Metrics: model.Metrics{
					{ShortName: "v1", Usage: "COUNTER", Value: "v1", Labels: []string{"l1"}, Description: "v1 description"},
				},
			},
			"example2": {
				Query: "SELECT 'label1' as l1, 'label2' as l2, 'label3' as l3, 1 as v1, 2 as v2",
				Metrics: model.Metrics{
					{ShortName: "v1", Usage: "COUNTER", Value: "v1", Labels: []string{"l1", "l2", "l3"}, Description: "v1 description"},
					{ShortName: "v2", Usage: "GAUGE", Value: "v2", Labels: []string{"l1", "l2", "l3"}, Description: "v2 description"},
				},
			},
		},
	}

	var input = pipelineInput{
		required: []string{
			"postgres_example1_v1",
			"postgres_example2_v1",
			"postgres_example2_v2",
		},
		collector:         NewPostgresCustomCollector,
		collectorSettings: settings,
		service:           model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}
