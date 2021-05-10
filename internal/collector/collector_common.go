package collector

import (
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"strings"
)

// typedDesc is the descriptor wrapper with extra properties
type typedDesc struct {
	// desc is the descriptor used by every Prometheus Metric.
	desc *prometheus.Desc
	// valueType is an enumeration of metric types that represent a simple value.
	valueType prometheus.ValueType
	// multiplier used to cast value to necessary units.
	factor float64
}

// mustNewConstMetric is the wrapper on prometheus.MustNewConstMetric
func (d *typedDesc) mustNewConstMetric(value float64, labels ...string) prometheus.Metric {
	if d.factor != 0 {
		value *= d.factor
	}
	return prometheus.MustNewConstMetric(d.desc, d.valueType, value, labels...)
}

// typedDescSet unions metrics in a set, which could be collected using query.
type typedDescSet struct {
	databases      []string             // list of databases from which metrics should be collected
	query          string               // query used for requesting stats
	variableLabels []string             // ordered list of labels names
	metricNames    []string             // ordered list of metrics short names (with no namespace/subsystem)
	descs          map[string]typedDesc // metrics descriptors
}

// newDescSet creates new typedDescSet based on passed metrics attributes.
func newDescSet(constLabels prometheus.Labels, namespace, subsystem string, settings model.MetricsSubsystem) typedDescSet {
	var variableLabels []string

	// Add extra "database" label to metrics collected from different databases.
	if len(settings.Databases) > 0 {
		variableLabels = append(variableLabels, "database")
	}

	// Construct the rest of labels slice.
	for _, m := range settings.Metrics {
		if m.Usage == "LABEL" {
			variableLabels = append(variableLabels, m.ShortName)
		}
	}

	descs := make(map[string]typedDesc)

	// typeMap is auxiliary dictionary for selecting proper Prometheus data type depending on 'usage' property.
	typeMap := map[string]prometheus.ValueType{
		"COUNTER": prometheus.CounterValue,
		"GAUGE":   prometheus.GaugeValue,
	}

	// Construct metrics names and descriptors slices.
	var metricNames []string
	for _, m := range settings.Metrics {
		if m.Usage == "LABEL" {
			continue
		}

		metricNames = append(metricNames, m.ShortName)

		metricName := prometheus.BuildFQName(namespace, subsystem, m.ShortName)
		d := typedDesc{
			desc: prometheus.NewDesc(
				metricName,
				m.Description,
				variableLabels, constLabels,
			), valueType: typeMap[m.Usage],
		}

		descs[m.ShortName] = d
	}

	return typedDescSet{
		databases:      settings.Databases,
		query:          settings.Query,
		metricNames:    metricNames,
		variableLabels: variableLabels,
		descs:          descs,
	}
}

// newDeskSetsFromSubsystems parses subsystem object and produces []typedDescSet object.
func newDeskSetsFromSubsystems(namespace string, subsystems model.Subsystems, constLabels prometheus.Labels) []typedDescSet {
	var sets []typedDescSet

	// Iterate over all passed subsystems and create dedicated descs set per each subsystem.
	// Consider all metrics are in the 'postgres' namespace.
	for k, v := range subsystems {
		descset := newDescSet(constLabels, namespace, k, v)
		sets = append(sets, descset)
	}

	return sets
}

// updateAllDescSets collect metrics for specified desc set.
func updateAllDescSets(config Config, descSets []typedDescSet, ch chan<- prometheus.Metric) error {

	// Get de-duplicated list of databases should be visited.
	databases := listDeskSetDatabases(descSets)

	// Collect multiple-databases metrics.
	if len(databases) > 0 {
		err := updateFromMultipleDatabases(config, descSets, databases, ch)
		if err != nil {
			log.Errorf("collect failed: %s; skip", err)
		}
	}

	// Collect once-database metrics.
	err := updateFromSingleDatabase(config, descSets, ch)
	if err != nil {
		log.Errorf("collect failed: %s; skip", err)
	}

	return nil
}

// updateFromSingleDatabase method visit only one database and collect necessary metrics.
func updateFromSingleDatabase(config Config, descSets []typedDescSet, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Iterate over descs sets. Every set contains metrics and labels names, query used
	// for getting data and metrics descriptors. All these sufficient to request stats
	// and translate stats into metrics.

	for _, s := range descSets {
		// Skip sets with multiple databases.
		if len(s.databases) > 0 {
			continue
		}

		err = updateSingleDescSet(conn, s, ch)
		if err != nil {
			log.Errorf("collect failed: %s; skip", err)
			continue
		}
	}

	return nil
}

// updateFromMultipleDatabases method visits all requested databases and collects necessary metrics.
func updateFromMultipleDatabases(config Config, descSets []typedDescSet, userDatabases []string, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}

	realDatabases, err := listDatabases(conn)
	if err != nil {
		return err
	}

	conn.Close()

	pgconfig, err := pgx.ParseConfig(config.ConnString)
	if err != nil {
		return err
	}

	// walk through all databases, connect to it and collect schema-specific stats
	for _, dbname := range userDatabases {
		// Skip user-specified databases which are not really exist.
		if !stringsContains(realDatabases, dbname) {
			continue
		}

		// Create
		pgconfig.Database = dbname
		conn, err := store.NewWithConfig(pgconfig)
		if err != nil {
			return err
		}

		for _, s := range descSets {
			// Skip sets with single databases, and databases which are not listed in set's databases.
			if len(s.databases) == 0 || !stringsContains(s.databases, dbname) {
				continue
			}

			// Swap descriptors labels, add database as first label
			if len(s.variableLabels) > 0 && s.variableLabels[0] != "database" {
				s.variableLabels = append([]string{"database"}, s.variableLabels...)
			}

			err = updateSingleDescSet(conn, s, ch)
			if err != nil {
				log.Errorf("collect failed: %s; skip", err)
				continue
			}
		}

		// Close connection.
		conn.Close()
	}

	return nil
}

// updateSingleDescSet using passed connection collects metrics for specified descSet and receives it to metric channel.
func updateSingleDescSet(conn *store.DB, set typedDescSet, ch chan<- prometheus.Metric) error {
	res, err := conn.Query(set.query)
	if err != nil {
		return err
	}

	stats := parsePostgresCustomStats(res, set.variableLabels)

	// Get database name from config.
	// Database name used as value for 'database' label in case of
	// user-defined metrics collected from multiple databases.
	dbname := conn.Conn().Config().Database

	// iterate over stats, extract labels and values, wrap to metric and send to receiver.
	for key, stat := range stats {
		// If database label present in variable labels, prepend label values with database name.
		var labelValues []string
		if len(set.variableLabels) > 0 && set.variableLabels[0] == "database" {
			if key != "" {
				labelValues = append([]string{dbname}, strings.Split(key, "/")...)
			} else {
				labelValues = []string{dbname}
			}
		} else {
			if key != "" {
				labelValues = strings.Split(key, "/")
			}
		}

		for name, value := range stat {
			d := set.descs[name]
			ch <- d.mustNewConstMetric(value, labelValues...)
		}
	}

	return nil
}

// listDeskSetDatabases parses slice of descSet and returns de-duplicated list of databases.
func listDeskSetDatabases(sets []typedDescSet) []string {
	// Make list of databases should be visited for collecting metrics.
	m := map[string]bool{}
	for _, set := range sets {
		for _, dbname := range set.databases {
			m[dbname] = true
		}
	}

	databases := []string{}
	for dbname := range m {
		databases = append(databases, dbname)
	}

	return databases
}

// removeCollisions looking for metrics with the same name in subsystems with the same name.
func removeCollisions(s1 model.Subsystems, s2 model.Subsystems) {
	for name, subsys1 := range s1 {
		if subsys2, ok := s2[name]; ok {

			for _, m1 := range subsys1.Metrics {
				for _, m2 := range subsys2.Metrics {
					if m1.ShortName == m2.ShortName {
						log.Warnf("ignore subsystem '%s': metric '%s' collision found. Check the configuration.", name, m2.ShortName)
						delete(s2, name)
					}
				}
			}
		}
	}
}
