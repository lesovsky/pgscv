package collector

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
)

// typedDesc is the descriptor wrapper with extra properties
type typedDesc struct {
	// name is the name of column in a query output used for getting value for metric.
	colname string
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

// lookupDesc returns index of the descriptor with colname specified in pattern
func lookupByColname(descs []typedDesc, pattern string) (int, error) {
	for i, desc := range descs {
		if desc.colname == pattern {
			return i, nil
		}
	}
	return -1, fmt.Errorf("pattern not found")
}
