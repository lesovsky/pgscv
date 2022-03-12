package collector

import (
	"github.com/lesovsky/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
)

// pgscvServicesCollector defines metrics about discovered and monitored services.
type pgscvServicesCollector struct {
	service typedDesc
}

// NewPgscvServicesCollector creates new collector.
func NewPgscvServicesCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	return &pgscvServicesCollector{
		service: newBuiltinTypedDesc(
			descOpts{"pgscv", "services", "registered_total", "Total number of services registered by pgSCV.", 0},
			prometheus.GaugeValue,
			[]string{"service"}, constLabels,
			settings.Filters,
		)}, nil
}

// Update method is used for sending pgscvServicesCollector's metrics.
func (c *pgscvServicesCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	ch <- c.service.newConstMetric(1, config.ServiceType)

	return nil
}
