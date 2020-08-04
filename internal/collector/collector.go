package collector

import (
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/prometheus/client_golang/prometheus"
	"sync"
)

// Factories defines collector functions which used for collecting metrics.
type Factories map[string]func(prometheus.Labels) (Collector, error)

// RegisterSystemCollectors unions all system-related collectors and registers them in single place.
func (f Factories) RegisterSystemCollectors() {
	f.register("cpu", NewCPUCollector)
	f.register("disk", NewDiskstatsCollector)
	f.register("filesystem", NewFilesystemCollector)
	f.register("network", NewNetdevCollector)
	f.register("memory", NewMeminfoCollector)
	f.register("system", NewSystemCollector)
}

// RegisterPostgresCollectors unions all postgres-related collectors and registers them in single place.
func (f Factories) RegisterPostgresCollectors() {
	f.register("activity", NewPostgresActivityCollector)
	f.register("bgwriter", NewPostgresBgwriterCollector)
	f.register("conflicts", NewPostgresConflictsCollector)
	f.register("database", NewPostgresDatabasesCollector)
	f.register("index", NewPostgresIndexesCollector)
	f.register("function", NewPostgresFunctionsCollector)
	f.register("replication", NewPostgresReplicationCollector)
	f.register("replication_slot", NewPostgresReplicationSlotCollector)
	f.register("statements", NewPostgresStatementsCollector)
	f.register("schema", NewPostgresSchemaCollector)
	f.register("setting", NewPostgresSettingsCollector)
	f.register("storage", NewPostgresStorageCollector)
	f.register("table", NewPostgresTablesCollector)
}

// RegisterPgbouncerCollectors unions all pgbouncer-related collectors and registers them in single place.
func (f Factories) RegisterPgbouncerCollectors() {
	f.register("pool", NewPgbouncerPoolsCollector)
	f.register("stats", NewPgbouncerStatsCollector)
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

// Collector implements the prometheus.Collector interface.
type PgscvCollector struct {
	Config     Config
	Collectors map[string]Collector
}

// NewPgscvCollector accepts Factories and creates per-service instance of Collector.
func NewPgscvCollector(projectID string, serviceID string, factories Factories, config Config) (*PgscvCollector, error) {
	collectors := make(map[string]Collector)
	constLabels := prometheus.Labels{"project_id": projectID, "service_id": serviceID}

	for key := range factories {
		collector, err := factories[key](constLabels)
		if err != nil {
			return nil, err
		}
		collectors[key] = collector
	}

	return &PgscvCollector{Config: config, Collectors: collectors}, nil
}

// Describe implements the prometheus.Collector interface.
func (n PgscvCollector) Describe(_ chan<- *prometheus.Desc) {}

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
	err := c.Update(config, ch)
	if err != nil {
		log.Errorf("%s collector failed; %s", name, err)
	}
}
