package collector

import (
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/prometheus/client_golang/prometheus"
	"sync"
	"time"
)

var (
//factories = make(map[string]func(prometheus.Labels) (Collector, error))
)

var (
	scrapeDurationDesc = prometheus.NewDesc(
		prometheus.BuildFQName("pgscv", "scrape", "collector_duration_seconds"),
		"pgscv_collector: Duration of a collector scrape.",
		[]string{"collector"},
		nil,
	)
	scrapeSuccessDesc = prometheus.NewDesc(
		prometheus.BuildFQName("pgscv", "scrape", "collector_success"),
		"node_collector: Whether a collector succeeded.",
		[]string{"collector"},
		nil,
	)
)

type Factories map[string]func(prometheus.Labels) (Collector, error)

// Collector is the interface a collector has to implement.
type Collector interface {
	// Get new metrics and expose them via prometheus registry.
	Update(ch chan<- prometheus.Metric) error
}

// Collector implements the prometheus.Collector interface.
type PgscvCollector struct {
	Collectors map[string]Collector
}

func NewPgscvCollector(projectID string, serviceID string, factories Factories) (*PgscvCollector, error) {
	collectors := make(map[string]Collector)

	for key := range factories {
		collector, err := factories[key](prometheus.Labels{"project_id": projectID, "service_id": serviceID})
		if err != nil {
			return nil, err
		}
		collectors[key] = collector
	}

	return &PgscvCollector{Collectors: collectors}, nil
}

func registerCollector(factories Factories, collector string, factory func(prometheus.Labels) (Collector, error)) {
	factories[collector] = factory
}

func RegisterSystemCollectors(factories Factories) {
	registerCollector(factories, "cpu", NewCPUCollector)
}

// Describe implements the prometheus.Collector interface.
func (n PgscvCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- scrapeDurationDesc
	ch <- scrapeSuccessDesc
}

// Collect implements the prometheus.Collector interface.
func (n PgscvCollector) Collect(ch chan<- prometheus.Metric) {
	wg := sync.WaitGroup{}
	wg.Add(len(n.Collectors))
	for name, c := range n.Collectors {
		go func(name string, c Collector) {
			execute(name, c, ch)
			wg.Done()
		}(name, c)
	}
	wg.Wait()
}

func execute(name string, c Collector, ch chan<- prometheus.Metric) {
	begin := time.Now()
	err := c.Update(ch)
	duration := time.Since(begin)
	var success float64

	if err != nil {
		log.Errorf("%s collector failed; duration_seconds %f; err: %s", name, duration.Seconds(), err)
		success = 0
	} else {
		success = 1
	}
	ch <- prometheus.MustNewConstMetric(scrapeDurationDesc, prometheus.GaugeValue, duration.Seconds(), name)
	ch <- prometheus.MustNewConstMetric(scrapeSuccessDesc, prometheus.GaugeValue, success, name)
}
