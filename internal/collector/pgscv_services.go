package collector

import "github.com/prometheus/client_golang/prometheus"

// pgscvServicesCollector defines metrics about discovered and monitored services.
type pgscvServicesCollector struct {
	service typedDesc
}

// NewPgscvServicesCollector creates new collector.
func NewPgscvServicesCollector(labels prometheus.Labels) (Collector, error) {
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
	ch <- c.service.mustNewConstMetric(1, config.ServiceType)

	return nil
}
