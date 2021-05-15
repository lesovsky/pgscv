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
	// value defines column name where metric value should be collected
	value string
	// labeledValues defines pairs with labelname:[]column_name,
	// where column name used as label values, column values used as metric values
	labeledValues map[string][]string
	// list of all metric labels (including those from labeledValues)
	labels []string
}

// newConstMetric is the wrapper on prometheus.NewConstMetric
func (d *typedDesc) newConstMetric(value float64, labels ...string) prometheus.Metric {
	if d.factor != 0 {
		value *= d.factor
	}

	m, err := prometheus.NewConstMetric(d.desc, d.valueType, value, labels...)
	if err != nil {
		log.Errorf("create const metric failed: %s; skip. Failed metric descriptor: '%s'", err, d.desc.String())
	}

	return m
}

// typedDescSet unions metrics in a set, which could be collected using query.
type typedDescSet struct {
	namespace   string         // namespace to which all nested metrics are belong
	subsystem   string         // subsystem to which all nested metrics are belong
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

	// Compile regexp object if databases are specified
	var databasesRE *regexp.Regexp
	if subsystem.Databases != "" {
		var err error
		databasesRE, err = regexp.Compile(subsystem.Databases)
		if err != nil {
			return typedDescSet{}, err
		}
	}

	promValueTypes := map[string]prometheus.ValueType{
		"COUNTER": prometheus.CounterValue,
		"GAUGE":   prometheus.GaugeValue,
	}

	var descs []typedDesc
	for _, m := range subsystem.Metrics {

		// When particular databases specified in user-defined metrics, add 'database' label for metric labels.
		var labels []string
		if subsystem.Databases != "" {
			labels = append([]string{"database"}, m.Labels...)
		} else {
			labels = m.Labels
		}

		// Append label names for labeled values.
		for k := range m.LabeledValues {
			labels = append(labels, k)
		}

		if m.Value == "" && m.LabeledValues == nil {
			log.Warnf("metric '%s' values of 'value' or 'labeledValues' must not be empty; skip", m.ShortName)
			continue
		}

		if _, ok := promValueTypes[m.Usage]; !ok {
			log.Warnf("metric '%s' value of 'usage' is unknown: %s; skip", m.ShortName, m.Usage)
			continue
		}

		d := typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName(namespace, subsystemName, m.ShortName),
				m.Description,
				labels,
				constLabels,
			),
			valueType:     promValueTypes[m.Usage],
			value:         m.Value,
			labeledValues: m.LabeledValues,
			labels:        labels,
		}

		descs = append(descs, d)
	}

	return typedDescSet{
		namespace:   namespace,
		subsystem:   subsystemName,
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
			updateMetrics(row, d, colnames, ch, databaseLabelValue)
		}
	}

	return nil
}

// updateMetrics
func updateMetrics(row []sql.NullString, desc typedDesc, colnames []string, ch chan<- prometheus.Metric, databaseLabelValue string) {
	// Using the descriptor a many metrics could be produced (with different label values).

	// When labeled values specified, it means a set of metrics returned.
	if desc.labeledValues != nil {
		updateMultipleMetrics(row, desc, colnames, ch, databaseLabelValue)
		return
	}

	updateSingleMetric(row, desc, colnames, ch, databaseLabelValue)
}

// updateMultipleMetrics parses data row and update multiple metrics using passed metric descriptor.
func updateMultipleMetrics(row []sql.NullString, desc typedDesc, colnames []string, ch chan<- prometheus.Metric, databaseLabelValue string) {
	initialLabelValues := []string{}

	// Insert into labels passed database name in case when there is no 'database' value in data row.
	if databaseLabelValue != "" && !stringsContains(colnames, "database") {
		initialLabelValues = []string{databaseLabelValue}
	}

	for _, valueCols := range desc.labeledValues { // walk through all labeledValues pairs
		for _, descColname := range valueCols { // walk through column names from labeledValues of metric descriptor

			labelValues, labelValuesOK := append([]string{}, initialLabelValues...), false
			value, valueOK := float64(0), false

			// Sanity check. Can't imaging such case when this condition is satisfied, but who knows...
			if len(labelValues) == len(desc.labels) {
				labelValuesOK = true
			}

			for i, resColname := range colnames { // walk through column names from data row
				// Check for value.
				sourceName, destName := parseLabeledValue(descColname)

				if sourceName == resColname && !valueOK {
					// Skip NULL values, metric must not be unknown (NULL).
					if !row[i].Valid {
						continue
					}

					var err error
					value, err = strconv.ParseFloat(row[i].String, 64)
					if err != nil {
						log.Errorf("invalid input, parse '%s' failed: %s; skip", row[i].String, err)
						continue
					}

					// When value found also update associated label.
					labelValues = append(labelValues, destName)
					if len(labelValues) == len(desc.labels) {
						labelValuesOK = true
					}
					valueOK = true

					continue
				}

				// Check for rest labels.
				if stringsContains(desc.labels, resColname) && !labelValuesOK {
					labelValues = append(labelValues, row[i].String)

					if len(labelValues) == len(desc.labels) {
						labelValuesOK = true
					}
				}
			}

			// Update metric only when value and all necessary labels are collected.

			if !valueOK || !labelValuesOK {
				log.Warnln("metric value or labels are not collected, skip")
				continue
			}

			ch <- desc.newConstMetric(value, labelValues...)
		}
	}
}

// updateSingleMetric parses data row and update single metric using passed metric descriptor.
func updateSingleMetric(row []sql.NullString, desc typedDesc, colnames []string, ch chan<- prometheus.Metric, databaseLabelValue string) {
	labelValues, labelValuesOK := []string{}, false
	value, valueOK := float64(0), false

	// Insert into labels passed database name in case when there is no 'database' value in data row.
	if databaseLabelValue != "" && !stringsContains(colnames, "database") {
		labelValues = append(labelValues, databaseLabelValue)
	}
	if len(labelValues) == len(desc.labels) {
		labelValuesOK = true
	}

	// Case when metric doesn't have any labels.
	if desc.labels == nil {
		labelValues = nil
		labelValuesOK = true
	}

	for i, colname := range colnames {
		// Check for value.
		if colname == desc.value {
			// Skip NULL values - metric must not be unknown (NULL)
			if !row[i].Valid {
				continue
			}

			var err error
			value, err = strconv.ParseFloat(row[i].String, 64)
			if err != nil {
				log.Errorf("invalid input, parse '%s' failed: %s; skip", row[i].String, err)
				continue
			}

			valueOK = true
			continue
		}

		// Check for labels.
		if stringsContains(desc.labels, colname) {
			labelValues = append(labelValues, row[i].String)

			if len(labelValues) == len(desc.labels) {
				labelValuesOK = true
			}
			continue
		}
	}

	// Update metric only when value and all necessary labels are collected.

	if !valueOK || !labelValuesOK {
		log.Warnln("metric value or labels are not collected, skip")
		return
	}

	ch <- desc.newConstMetric(value, labelValues...)
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

// parseLabeledValue parses value from labeledValues and return source and destination labels.
func parseLabeledValue(s string) (string, string) {
	if s == "" {
		return "", ""
	}

	ff := strings.Split(s, "/")
	if len(ff) == 1 {
		return ff[0], ff[0]
	}

	return ff[0], ff[1]
}
