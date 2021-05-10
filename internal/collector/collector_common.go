package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/model"
)

// typedDesc is the descriptor wrapper with extra properties
type typedDesc struct {
	// desc is the descriptor used by every Prometheus Metric.
	desc *prometheus.Desc
	// valueType is an enumeration of metric types that represent a simple value.
	valueType prometheus.ValueType
	// multiplier used to cast value to necessary units.
	factor float64
}

// mustNewConstMetric is the wrapper on prometheus.MustNewConstMetric
func (d *typedDesc) mustNewConstMetric(value float64, labels ...string) prometheus.Metric {
	if d.factor != 0 {
		value *= d.factor
	}
	return prometheus.MustNewConstMetric(d.desc, d.valueType, value, labels...)
}

// typedDescSet unions metrics in a set, which could be collected using query.
type typedDescSet struct {
	databases      []string             // list of databases from which metrics should be collected
	query          string               // query used for requesting stats
	variableLabels []string             // ordered list of labels names
	metricNames    []string             // ordered list of metrics short names (with no namespace/subsystem)
	descs          map[string]typedDesc // metrics descriptors
}

// newDescSet creates new typedDescSet based on passed metrics attributes.
func newDescSet(constLabels prometheus.Labels, namespace, subsystem string, settings model.MetricsSubsystem) typedDescSet {
	var variableLabels []string

	// Add extra "database" label to metrics collected from different databases.
	if len(settings.Databases) > 0 {
		variableLabels = append(variableLabels, "database")
	}

	// Construct the rest of labels slice.
	for _, m := range settings.Metrics {
		if m.Usage == "LABEL" {
			variableLabels = append(variableLabels, m.ShortName)
		}
	}

	descs := make(map[string]typedDesc)

	// typeMap is auxiliary dictionary for selecting proper Prometheus data type depending on 'usage' property.
	typeMap := map[string]prometheus.ValueType{
		"COUNTER": prometheus.CounterValue,
		"GAUGE":   prometheus.GaugeValue,
	}

	// Construct metrics names and descriptors slices.
	var metricNames []string
	for _, m := range settings.Metrics {
		if m.Usage == "LABEL" {
			continue
		}

		metricNames = append(metricNames, m.ShortName)

		metricName := prometheus.BuildFQName(namespace, subsystem, m.ShortName)
		d := typedDesc{
			desc: prometheus.NewDesc(
				metricName,
				m.Description,
				variableLabels, constLabels,
			), valueType: typeMap[m.Usage],
		}

		descs[m.ShortName] = d
	}

	return typedDescSet{
		databases:      settings.Databases,
		query:          settings.Query,
		metricNames:    metricNames,
		variableLabels: variableLabels,
		descs:          descs,
	}
}
