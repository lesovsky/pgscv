package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/weaponry/pgscv/internal/model"
	"testing"
)

func Test_newDescSet(t *testing.T) {
	testcases := []struct {
		subsystem       model.MetricsSubsystem
		wantQuery       string
		wantVarLabels   []string
		wantMetricNames []string
	}{
		{
			// With no databases specified
			subsystem: model.MetricsSubsystem{
				Query: "SELECT 'l1' as label1, 'l2' as label2, 0.123 as metric1, 0.456 as metric2, 0.789 as metric3",
				Metrics: model.Metrics{
					{ShortName: "label1", Usage: "LABEL", Description: "label1 description"},
					{ShortName: "label2", Usage: "LABEL", Description: "label2 description"},
					{ShortName: "metric1", Usage: "GAUGE", Description: "metric1 description"},
					{ShortName: "metric2", Usage: "GAUGE", Description: "metric2 description"},
					{ShortName: "metric3", Usage: "GAUGE", Description: "metric3 description"},
				},
			},
			wantQuery:       "SELECT 'l1' as label1, 'l2' as label2, 0.123 as metric1, 0.456 as metric2, 0.789 as metric3",
			wantVarLabels:   []string{"label1", "label2"},
			wantMetricNames: []string{"metric1", "metric2", "metric3"},
		},
		{
			// With databases specified
			subsystem: model.MetricsSubsystem{
				Databases: []string{"pgscv_fixtures"},
				Query:     "SELECT 'l1' as label1, 'l2' as label2, 0.123 as metric1, 0.456 as metric2, 0.789 as metric3",
				Metrics: model.Metrics{
					{ShortName: "label1", Usage: "LABEL", Description: "label1 description"},
					{ShortName: "label2", Usage: "LABEL", Description: "label2 description"},
					{ShortName: "metric1", Usage: "GAUGE", Description: "metric1 description"},
					{ShortName: "metric2", Usage: "GAUGE", Description: "metric2 description"},
					{ShortName: "metric3", Usage: "GAUGE", Description: "metric3 description"},
				},
			},
			wantQuery:       "SELECT 'l1' as label1, 'l2' as label2, 0.123 as metric1, 0.456 as metric2, 0.789 as metric3",
			wantVarLabels:   []string{"database", "label1", "label2"},
			wantMetricNames: []string{"metric1", "metric2", "metric3"},
		},
	}

	constLabels := prometheus.Labels{"constlabel": "example"}

	for _, tc := range testcases {
		descSet := newDescSet(constLabels, "postgres", "class", tc.subsystem)
		assert.Equal(t, tc.wantQuery, descSet.query)
		assert.Equal(t, tc.wantVarLabels, descSet.variableLabels)
		assert.Equal(t, tc.wantMetricNames, descSet.metricNames)
		assert.NotNil(t, descSet.descs)
	}
}
