package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/model"
)

type postgresCustomCollector struct {
	custom []typedDescSet
}

// NewPostgresCustomCollector returns a new Collector that expose user-defined postgres metrics.
func NewPostgresCustomCollector(constLabels prometheus.Labels, settings model.CollectorSettings) (Collector, error) {
	return &postgresCustomCollector{
		custom: newDeskSetsFromSubsystems("postgres", settings.Subsystems, constLabels),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresCustomCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	return updateAllDescSets(config, c.custom, ch)
}
