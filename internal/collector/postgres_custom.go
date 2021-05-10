package collector

import (
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"strings"
)

type postgresCustomCollector struct {
	descSets []typedDescSet
}

// NewPostgresCustomCollector returns a new Collector that expose user-defined postgres metrics.
func NewPostgresCustomCollector(constLabels prometheus.Labels, settings model.CollectorSettings) (Collector, error) {
	var sets []typedDescSet

	// Iterate over all passed subsystems and create dedicated descs set per each subsystem.
	// Consider all metrics are in the 'postgres' namespace.
	for k, v := range settings.Subsystems {
		descset := newDescSet(constLabels, "postgres", k, v)
		sets = append(sets, descset)
	}

	return &postgresCustomCollector{
		descSets: sets,
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresCustomCollector) Update(config Config, ch chan<- prometheus.Metric) error {

	// Make list of databases should be visited for collecting metrics.
	databases := []string{}
	for _, set := range c.descSets {
		m := map[string]bool{}
		for _, dbname := range set.databases {
			m[dbname] = true
		}

		for dbname := range m {
			databases = append(databases, dbname)
		}
	}

	// Collect multiple-databases metrics.
	if len(databases) > 0 {
		err := c.updateFromMultipleDatabases(config, databases, ch)
		if err != nil {
			log.Errorf("collect failed: %s; skip", err)
		}
	}

	// Collect once-database metrics.
	err := c.updateFromSingleDatabase(config, ch)
	if err != nil {
		log.Errorf("collect failed: %s; skip", err)
	}

	return nil
}

// updateFromSingleDatabase method visit only one database and collect necessary metrics.
func (c *postgresCustomCollector) updateFromSingleDatabase(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Iterate over descs sets. Every set contains metrics and labels names, query used
	// for getting data and metrics descriptors. All these sufficient to request stats
	// and translate stats into metrics.

	for _, s := range c.descSets {
		// Skip sets with multiple databases.
		if len(s.databases) > 0 {
			continue
		}

		err = updateDescSet(conn, s, ch)
		if err != nil {
			log.Errorf("collect failed: %s; skip", err)
			continue
		}
	}

	return nil
}

// updateFromMultipleDatabases method visits all requested databases and collects necessary metrics.
func (c *postgresCustomCollector) updateFromMultipleDatabases(config Config, userDatabases []string, ch chan<- prometheus.Metric) error {
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

		for _, s := range c.descSets {
			// Skip sets with single databases, and databases which are not listed in set's databases.
			if len(s.databases) == 0 || !stringsContains(s.databases, dbname) {
				continue
			}

			// Swap descriptors labels, add database as first label
			if len(s.variableLabels) > 0 && s.variableLabels[0] != "database" {
				s.variableLabels = append([]string{"database"}, s.variableLabels...)
			}

			err = updateDescSet(conn, s, ch)
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

// updateDescSet using passed connection collects metrics for specified descSet and receives it to metric channel.
func updateDescSet(conn *store.DB, set typedDescSet, ch chan<- prometheus.Metric) error {
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
			labelValues = append([]string{dbname}, strings.Split(key, "/")...)
		} else {
			labelValues = strings.Split(key, "/")
		}

		for name, value := range stat {
			d := set.descs[name]
			ch <- d.mustNewConstMetric(value, labelValues...)
		}
	}

	return nil
}
