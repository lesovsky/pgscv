package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/model"
)

// pgscvServicesCollector defines metrics about discovered and monitored services.
type pgscvServicesCollector struct {
	service typedDesc
}

// NewPgscvServicesCollector creates new collector.
func NewPgscvServicesCollector(labels prometheus.Labels, _ model.CollectorSettings) (Collector, error) {
	return &pgscvServicesCollector{
		service: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("pgscv", "services", "registered_total"),
				"Total number of services registered by pgSCV.",
				[]string{"service"}, labels,
			), valueType: prometheus.GaugeValue,
		}}, nil
}

// Update method is used for sending pgscvServicesCollector's metrics.
func (c *pgscvServicesCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	ch <- c.service.newConstMetric(1, config.ServiceType)

	return nil
}
