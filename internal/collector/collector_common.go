package collector

import (
	"database/sql"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"regexp"
	"strconv"
)

// typedDesc is the descriptor wrapper with extra properties
type typedDesc struct {
	// desc is the descriptor used by every Prometheus Metric.
	desc *prometheus.Desc
	// valueType is an enumeration of metric types that represent a simple value.
	valueType prometheus.ValueType
	// multiplier used to cast value to necessary units.
	factor float64
	// value defines column name where metric value should be collected
	value string
	// labeledValues defines pairs with labelname:[]column_name,
	// where column name used as label values, column values used as metric values
	labeledValues map[string][]string
	// list of all metric labels (including those from labeledValues)
	labels []string
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
	databasesRE *regexp.Regexp // compiled regexp.Regexp object with databases from which metrics should be collected
	query       string         // query used for requesting stats
	descs       []typedDesc    // metrics descriptors
}

// newDeskSetsFromSubsystems parses subsystem object and produces []typedDescSet object.
func newDeskSetsFromSubsystems(namespace string, subsystems model.Subsystems, constLabels prometheus.Labels) []typedDescSet {
	var sets []typedDescSet

	// Iterate over all passed subsystems and create dedicated descs set per each subsystem.
	// Consider all metrics are in the 'postgres' namespace.
	for subsystemName, subsystem := range subsystems {
		descs, err := newDescSet(namespace, subsystemName, subsystem, constLabels)
		if err != nil {
			log.Warnf("create metrics descriptors set failed: %s; skip", err)
		}
		sets = append(sets, descs)
	}

	return sets
}

// newDescSet creates new typedDescSet based on passed metrics attributes.
func newDescSet(namespace string, subsystemName string, subsystem model.MetricsSubsystem, constLabels prometheus.Labels) (typedDescSet, error) {
	// Add extra "database" label to metrics collected from different databases.
	var databasesRE *regexp.Regexp
	if subsystem.Databases != "" {
		//variableLabels = append(variableLabels, "database")

		var err error
		databasesRE, err = regexp.Compile(subsystem.Databases)
		if err != nil {
			return typedDescSet{}, err
		}
	}

	promtypes := map[string]prometheus.ValueType{
		"COUNTER": prometheus.CounterValue,
		"GAUGE":   prometheus.GaugeValue,
	}

	var descs []typedDesc
	for _, m := range subsystem.Metrics {

		// формируем метки для метрики. Если юзер указал собирать метрики с отдельных баз, то в метки следует добавить метку с именем бд - database
		var labels []string
		if subsystem.Databases != "" {
			labels = append([]string{"database"}, m.Labels...)
		} else {
			labels = m.Labels
		}

		// append label names for labeled values
		for k := range m.LabeledValues {
			labels = append(labels, k)
		}

		d := typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName(namespace, subsystemName, m.ShortName),
				m.Description,
				labels, constLabels,
			),
			valueType:     promtypes[m.Usage],
			value:         m.Value,
			labeledValues: m.LabeledValues,
			labels:        labels,
		}

		descs = append(descs, d)
	}

	return typedDescSet{
		databasesRE: databasesRE,
		query:       subsystem.Query,
		descs:       descs,
	}, nil
}

// updateAllDescSets collect metrics for specified desc set.
func updateAllDescSets(config Config, descSets []typedDescSet, ch chan<- prometheus.Metric) error {
	// Collect multiple-databases metrics.
	if needMultipleUpdate(descSets) {
		err := updateFromMultipleDatabases(config, descSets, ch)
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

// updateFromMultipleDatabases method visits all requested databases and collects necessary metrics.
func updateFromMultipleDatabases(config Config, descSets []typedDescSet, ch chan<- prometheus.Metric) error {
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
	for _, dbname := range realDatabases {
		for _, s := range descSets {
			// Skip sets with update on single database, and databases which are not matched to user-defined databases.
			if s.databasesRE == nil || !s.databasesRE.MatchString(dbname) {
				continue
			}

			// Connect to the database and update metrics.
			pgconfig.Database = dbname
			conn, err := store.NewWithConfig(pgconfig)
			if err != nil {
				return err
			}

			err = updateSingleDescSet(conn, s, ch, true)
			if err != nil {
				log.Errorf("collect failed: %s; skip", err)
				conn.Close()
				continue
			}

			// Close connection.
			conn.Close()
		}
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
		if s.databasesRE != nil {
			continue
		}

		err = updateSingleDescSet(conn, s, ch, false)
		if err != nil {
			log.Errorf("collect failed: %s; skip", err)
			continue
		}
	}

	return nil
}

// updateSingleDescSet requests data using passed connection, parses returned result and update metrics in passed descs.
func updateSingleDescSet(conn *store.DB, descs typedDescSet, ch chan<- prometheus.Metric, addDatabaseLabel bool) error {
	res, err := conn.Query(descs.query)
	if err != nil {
		return err
	}

	colnames := []string{}
	for _, colname := range res.Colnames {
		colnames = append(colnames, string(colname.Name))
	}

	var databaseLabelValue string
	if addDatabaseLabel {
		databaseLabelValue = conn.Conn().Config().Database
	}

	for _, row := range res.Rows {
		for _, d := range descs.descs {
			parseRow(row, colnames, d, ch, databaseLabelValue)
		}
	}

	return nil
}

// parseRow parses row's content of PGresult and update metrics described in passed desc.
func parseRow(row []sql.NullString, colnames []string, desc typedDesc, ch chan<- prometheus.Metric, databaseLabelValue string) {
	// вопрос - сколько метрик надо отправить в рамках этого desc?
	// 1. если Value != "", то 1
	// 2. если LabeledValues != nil, то len(labeledValues)

	initialLabelValues, labelValues, labelValuesOK := []string{}, []string{}, false
	value, valueOK := float64(0), false

	// TODO: databaseLabelValue может быть передано, при этом в метрике также может быть метка database.
	if databaseLabelValue != "" {
		initialLabelValues = []string{databaseLabelValue}
		labelValues = append(labelValues, initialLabelValues...)
	}
	if len(labelValues) == len(desc.labels) {
		labelValuesOK = true
	}

	var err error
	if desc.value != "" {
		// метрика может не иметь меток совсем
		if desc.labels == nil {
			labelValues = nil
			labelValuesOK = true
		}

		for i, colname := range colnames {
			// check for value
			if colname == desc.value {
				value, err = strconv.ParseFloat(row[i].String, 64)
				if err != nil {
					log.Errorf("invalid input, parse '%s' failed: %s; skip", row[i].String, err)
					continue
				}

				valueOK = true
				continue
			}

			// check for labels
			if stringsContains(desc.labels, colname) {
				labelValues = append(labelValues, row[i].String)

				if len(labelValues) == len(desc.labels) {
					labelValuesOK = true
				}
				continue
			}
		}

		if valueOK && labelValuesOK {
			ch <- desc.mustNewConstMetric(value, labelValues...)
			labelValues, labelValuesOK = append([]string{}, initialLabelValues...), false
			value, valueOK = float64(0), false
		}
	}

	// case for labeledValues
	if desc.labeledValues != nil {
		for _, valueCols := range desc.labeledValues {
			for _, col := range valueCols { // имена колонок labeledValues
				for i, colname := range colnames { // имена колонок из реального запроса
					// check for value
					if col == colname && !valueOK {
						value, err = strconv.ParseFloat(row[i].String, 64)
						if err != nil {
							log.Errorf("invalid input, parse '%s' failed: %s; skip", row[i].String, err)
							continue
						}

						labelValues = append(labelValues, col)
						if len(labelValues) == len(desc.labels) {
							labelValuesOK = true
						}
						valueOK = true

						continue
					}

					// check for rest labels
					if stringsContains(desc.labels, colname) && !labelValuesOK {
						labelValues = append(labelValues, row[i].String)

						if len(labelValues) == len(desc.labels) {
							labelValuesOK = true
						}
					}
				}

				if valueOK && labelValuesOK {
					ch <- desc.mustNewConstMetric(value, labelValues...)
					labelValues, labelValuesOK = append([]string{}, initialLabelValues...), false
					value, valueOK = float64(0), false
				}
			}
		}
	}
}

// needMultipleUpdate returns true if databases regexp has been found.
func needMultipleUpdate(sets []typedDescSet) bool {
	for _, set := range sets {
		if set.databasesRE != nil {
			return true
		}
	}

	return false
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
