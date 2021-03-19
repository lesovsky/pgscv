package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"os"
	"sync"
)

// Factories defines collector functions which used for collecting metrics.
type Factories map[string]func(prometheus.Labels) (Collector, error)

// RegisterSystemCollectors unions all system-related collectors and registers them in single place.
func (f Factories) RegisterSystemCollectors(disabled []string) {
	if stringsContains(disabled, "system") {
		log.Debugln("disable all system collectors")
		return
	}

	funcs := map[string]func(prometheus.Labels) (Collector, error){
		"system/pgscv":       NewPgscvServicesCollector,
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

	funcs := map[string]func(prometheus.Labels) (Collector, error){
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

	funcs := map[string]func(prometheus.Labels) (Collector, error){
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
	// anchorDesc is a metric descriptor used for distinguishing collectors when unregister is required.
	anchorDesc typedDesc
}

// NewPgscvCollector accepts Factories and creates per-service instance of Collector.
func NewPgscvCollector(serviceID string, factories Factories, config Config) (*PgscvCollector, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	collectors := make(map[string]Collector)
	constLabels := prometheus.Labels{"instance": hostname, "service_id": serviceID}

	for key := range factories {
		collector, err := factories[key](constLabels)
		if err != nil {
			return nil, err
		}
		collectors[key] = collector
	}

	// anchorDesc is a metric descriptor used for distinguish collectors. Creating many collectors with uniq anchorDesc makes
	// possible to unregister collectors if they or their associated services become unnecessary or unavailable.
	desc := typedDesc{
		desc: prometheus.NewDesc(
			prometheus.BuildFQName("pgscv", "service", serviceID),
			"Service metric.",
			nil, constLabels,
		), valueType: prometheus.GaugeValue,
	}

	return &PgscvCollector{Config: config, Collectors: collectors, anchorDesc: desc}, nil
}

// Describe implements the prometheus.Collector interface.
func (n PgscvCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- n.anchorDesc.desc
}

// Collect implements the prometheus.Collector interface.
func (n PgscvCollector) Collect(out chan<- prometheus.Metric) {
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
