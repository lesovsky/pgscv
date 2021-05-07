package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/weaponry/pgscv/internal/model"
	"testing"
)

func Test_newDescSet(t *testing.T) {
	subsystem := model.MetricsSubsystem{
		Query: "SELECT 'l1' as label1, 'l2' as label2, 0.123 as metric1, 0.456 as metric2, 0.789 as metric3",
		Metrics: model.Metrics{
			{ShortName: "label1", Usage: "LABEL", Description: "label1 description"},
			{ShortName: "label2", Usage: "LABEL", Description: "label2 description"},
			{ShortName: "metric1", Usage: "GAUGE", Description: "metric1 description"},
			{ShortName: "metric2", Usage: "GAUGE", Description: "metric2 description"},
			{ShortName: "metric3", Usage: "GAUGE", Description: "metric3 description"},
		},
	}

	constLabels := prometheus.Labels{"constlabel": "example"}

	wantQuery := "SELECT 'l1' as label1, 'l2' as label2, 0.123 as metric1, 0.456 as metric2, 0.789 as metric3"
	wantVariableLabels := []string{"label1", "label2"}
	wantMetricNames := []string{"metric1", "metric2", "metric3"}

	descSet := newDescSet(constLabels, "postgres", "class", subsystem)
	assert.Equal(t, wantQuery, descSet.query)
	assert.Equal(t, wantVariableLabels, descSet.variableLabels)
	assert.Equal(t, wantMetricNames, descSet.metricNames)
	assert.NotNil(t, descSet.descs)
}
