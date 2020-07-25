package collector

import (
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/prometheus/client_golang/prometheus"
	"sync"
	"time"
)

var (
//scrapeDurationDesc = prometheus.NewDesc(
//	prometheus.BuildFQName("pgscv", "scrape", "collector_duration_seconds"),
//	"pgscv_collector: Duration of a collector scrape.",
//	[]string{"collector"},
//	nil,
//)
//scrapeSuccessDesc = prometheus.NewDesc(
//	prometheus.BuildFQName("pgscv", "scrape", "collector_success"),
//	"node_collector: Whether a collector succeeded.",
//	[]string{"collector"},
//	nil,
//)
)

// Factories defines collector functions which used for collecting metrics.
type Factories map[string]func(prometheus.Labels) (Collector, error)

// RegisterSystemCollectors unions all system-related collectors and registers them in single place.
func (f Factories) RegisterSystemCollectors() {
	f.register("cpu", NewCPUCollector)
	f.register("disk", NewDiskstatsCollector)
}

// RegisterPostgresCollectors unions all postgres-related collectors and registers them in single place.
func (f Factories) RegisterPostgresCollectors() {
	f.register("database", NewPostgresDatabasesCollector)
	f.register("table", NewPostgresTablesCollector)
	f.register("bgwriter", NewPostgresBgwriterCollector)
	f.register("function", NewPostgresFunctionsCollector)
}

// RegisterPgbouncerCollectors unions all pgbouncer-related collectors and registers them in single place.
func (f Factories) RegisterPgbouncerCollectors() {
	f.register("pool", NewPgbouncerPoolsCollector)
	f.register("pool", NewPgbouncerStatsCollector)
}

// register is the generic routine which register any kind of collectors.
func (f Factories) register(collector string, factory func(prometheus.Labels) (Collector, error)) {
	f[collector] = factory
}

// Collector is the interface a collector has to implement.
type Collector interface {
	// Get new metrics and expose them via prometheus registry.
	Update(config Config, ch chan<- prometheus.Metric) error
}

// Config defines collector configuration settings
type Config struct {
	ServiceType string
	ConnString  string
}

// Collector implements the prometheus.Collector interface.
type PgscvCollector struct {
	Config     Config
	Collectors map[string]Collector
}

// NewPgscvCollector accepts Factories and creates per-service instance of Collector.
func NewPgscvCollector(projectID string, serviceID string, factories Factories, config Config) (*PgscvCollector, error) {
	collectors := make(map[string]Collector)

	for key := range factories {
		collector, err := factories[key](prometheus.Labels{"project_id": projectID, "service_id": serviceID})
		if err != nil {
			return nil, err
		}
		collectors[key] = collector
	}

	return &PgscvCollector{Config: config, Collectors: collectors}, nil
}

// Describe implements the prometheus.Collector interface.
func (n PgscvCollector) Describe(_ chan<- *prometheus.Desc) {
	//ch <- scrapeDurationDesc
	//ch <- scrapeSuccessDesc
}

// Collect implements the prometheus.Collector interface.
func (n PgscvCollector) Collect(ch chan<- prometheus.Metric) {
	wg := sync.WaitGroup{}
	wg.Add(len(n.Collectors))
	for name, c := range n.Collectors {
		go func(name string, c Collector) {
			execute(name, n.Config, c, ch)
			wg.Done()
		}(name, c)
	}
	wg.Wait()
}

// execute acts like a middleware - it runs metric collection function and wraps it into instrumenting logic.
func execute(name string, config Config, c Collector, ch chan<- prometheus.Metric) {
	begin := time.Now()
	err := c.Update(config, ch)
	duration := time.Since(begin)
	//var success float64

	if err != nil {
		log.Errorf("%s collector failed; duration_seconds %f; err: %s", name, duration.Seconds(), err)
		//success = 0
	} else {
		//success = 1
	}
	//ch <- prometheus.MustNewConstMetric(scrapeDurationDesc, prometheus.GaugeValue, duration.Seconds(), name)
	//ch <- prometheus.MustNewConstMetric(scrapeSuccessDesc, prometheus.GaugeValue, success, name)
}
