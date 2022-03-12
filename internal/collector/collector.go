package collector

import (
	"github.com/lesovsky/pgscv/internal/filter"
	"github.com/lesovsky/pgscv/internal/log"
	"github.com/lesovsky/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"sync"
)

// Factories defines collector functions which used for collecting metrics.
type Factories map[string]func(labels, model.CollectorSettings) (Collector, error)

// RegisterSystemCollectors unions all system-related collectors and registers them in single place.
func (f Factories) RegisterSystemCollectors(disabled []string) {
	if stringsContains(disabled, "system") {
		log.Debugln("disable all system collectors")
		return
	}

	funcs := map[string]func(labels, model.CollectorSettings) (Collector, error){
		"system/pgscv":       NewPgscvServicesCollector,
		"system/sysinfo":     NewSysInfoCollector,
		"system/loadaverage": NewLoadAverageCollector,
		"system/cpu":         NewCPUCollector,
		"system/diskstats":   NewDiskstatsCollector,
		"system/filesystems": NewFilesystemCollector,
		"system/netdev":      NewNetdevCollector,
		"system/network":     NewNetworkCollector,
		"system/memory":      NewMeminfoCollector,
		"system/sysconfig":   NewSysconfigCollector,
	}

	for name, fn := range funcs {
		if stringsContains(disabled, name) {
			log.Debugln("disable ", name)
			continue
		}

		log.Debugln("enable ", name)
		f.register(name, fn)
	}
}

// RegisterPostgresCollectors unions all postgres-related collectors and registers them in single place.
func (f Factories) RegisterPostgresCollectors(disabled []string) {
	if stringsContains(disabled, "postgres") {
		log.Debugln("disable all postgres collectors")
		return
	}

	funcs := map[string]func(labels, model.CollectorSettings) (Collector, error){
		"postgres/pgscv":             NewPgscvServicesCollector,
		"postgres/activity":          NewPostgresActivityCollector,
		"postgres/archiver":          NewPostgresWalArchivingCollector,
		"postgres/bgwriter":          NewPostgresBgwriterCollector,
		"postgres/conflicts":         NewPostgresConflictsCollector,
		"postgres/databases":         NewPostgresDatabasesCollector,
		"postgres/indexes":           NewPostgresIndexesCollector,
		"postgres/functions":         NewPostgresFunctionsCollector,
		"postgres/locks":             NewPostgresLocksCollector,
		"postgres/logs":              NewPostgresLogsCollector,
		"postgres/replication":       NewPostgresReplicationCollector,
		"postgres/replication_slots": NewPostgresReplicationSlotsCollector,
		"postgres/statements":        NewPostgresStatementsCollector,
		"postgres/schemas":           NewPostgresSchemasCollector,
		"postgres/settings":          NewPostgresSettingsCollector,
		"postgres/storage":           NewPostgresStorageCollector,
		"postgres/tables":            NewPostgresTablesCollector,
		"postgres/wal":               NewPostgresWalCollector,
		"postgres/custom":            NewPostgresCustomCollector,
	}

	for name, fn := range funcs {
		if stringsContains(disabled, name) {
			log.Debugln("disable ", name)
			continue
		}
		log.Debugln("enable ", name)
		f.register(name, fn)
	}
}

// RegisterPgbouncerCollectors unions all pgbouncer-related collectors and registers them in single place.
func (f Factories) RegisterPgbouncerCollectors(disabled []string) {
	if stringsContains(disabled, "pgbouncer") {
		log.Debugln("disable all pgbouncer collectors")
		return
	}

	funcs := map[string]func(labels, model.CollectorSettings) (Collector, error){
		"pgbouncer/pgscv":    NewPgscvServicesCollector,
		"pgbouncer/pools":    NewPgbouncerPoolsCollector,
		"pgbouncer/stats":    NewPgbouncerStatsCollector,
		"pgbouncer/settings": NewPgbouncerSettingsCollector,
	}

	for name, fn := range funcs {
		if stringsContains(disabled, name) {
			log.Debugln("disable ", name)
			continue
		}

		log.Debugln("enable ", name)
		f.register(name, fn)
	}
}

// register is the generic routine which register any kind of collectors.
func (f Factories) register(collector string, factory func(labels, model.CollectorSettings) (Collector, error)) {
	f[collector] = factory
}

// Collector is the interface a collector has to implement.
type Collector interface {
	// Update does collecting new metrics and expose them via prometheus registry.
	Update(config Config, ch chan<- prometheus.Metric) error
}

// PgscvCollector implements the prometheus.Collector interface.
type PgscvCollector struct {
	Config     Config
	Collectors map[string]Collector
	// anchorDesc is a metric descriptor used for distinguishing collectors when unregister is required.
	anchorDesc typedDesc
}

// NewPgscvCollector accepts Factories and creates per-service instance of Collector.
func NewPgscvCollector(serviceID string, factories Factories, config Config) (*PgscvCollector, error) {
	collectors := make(map[string]Collector)
	constLabels := labels{"service_id": serviceID}

	for key := range factories {
		settings := config.Settings[key]

		collector, err := factories[key](constLabels, settings)
		if err != nil {
			return nil, err
		}
		collectors[key] = collector
	}

	// anchorDesc is a metric descriptor used for distinguish collectors. Creating many collectors with uniq anchorDesc makes
	// possible to unregister collectors if they or their associated services become unnecessary or unavailable.
	desc := newBuiltinTypedDesc(
		descOpts{"pgscv", "service", serviceID, "Service metric.", 0},
		prometheus.GaugeValue,
		nil, constLabels,
		filter.New(),
	)

	return &PgscvCollector{Config: config, Collectors: collectors, anchorDesc: desc}, nil
}

// Describe implements the prometheus.Collector interface.
func (n PgscvCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- n.anchorDesc.desc
}

// Collect implements the prometheus.Collector interface.
func (n PgscvCollector) Collect(out chan<- prometheus.Metric) {
	// Update settings of Postgres collectors
	if n.Config.ServiceType == "postgres" {
		cfg, err := newPostgresServiceConfig(n.Config.ConnString)
		if err != nil {
			log.Errorf("update service config failed: %s, skip collect", err.Error())
			return
		}

		n.Config.postgresServiceConfig = cfg
	}

	wgCollector := sync.WaitGroup{}
	wgSender := sync.WaitGroup{}

	// Create pipe channel used transmitting metrics from collectors to sender.
	pipelineIn := make(chan prometheus.Metric)

	// Run collectors.
	wgCollector.Add(len(n.Collectors))
	for name, c := range n.Collectors {
		go func(name string, c Collector) {
			collect(name, n.Config, c, pipelineIn)
			wgCollector.Done()
		}(name, c)
	}

	// Run sender.
	wgSender.Add(1)
	go func() {
		send(pipelineIn, out)
		wgSender.Done()
	}()

	// Wait until all collectors have been finished. Close the channel and allow to sender to send metrics.
	wgCollector.Wait()
	close(pipelineIn)

	// Wait until metrics have been sent.
	wgSender.Wait()
}

// send acts like a middleware between metric collector functions which produces metrics and Prometheus who accepts metrics.
func send(in <-chan prometheus.Metric, out chan<- prometheus.Metric) {
	for m := range in {
		// Skip received nil values
		if m == nil {
			continue
		}

		// implement other middlewares here.

		out <- m
	}
}

// collect runs metric collection function and wraps it into instrumenting logic.
func collect(name string, config Config, c Collector, ch chan<- prometheus.Metric) {
	err := c.Update(config, ch)
	if err != nil {
		log.Errorf("%s collector failed; %s", name, err)
	}
}
