package collector

import (
	"github.com/lesovsky/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
)

type postgresCustomCollector struct {
	custom []typedDescSet
}

// NewPostgresCustomCollector returns a new Collector that expose user-defined postgres metrics.
func NewPostgresCustomCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	return &postgresCustomCollector{
		custom: newDeskSetsFromSubsystems("postgres", settings.Subsystems, constLabels),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresCustomCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	return updateAllDescSets(config, c.custom, ch)
}
