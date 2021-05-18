package collector

import (
	"context"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"strconv"
	"strings"
)

const (
	// Postgres server versions numeric representations.
	PostgresV95 = 90500
	PostgresV96 = 90600
	PostgresV10 = 100000
	PostgresV12 = 120000
	PostgresV13 = 130000

	// Minimal required version is 9.5.
	PostgresVMinNum = PostgresV95
	PostgresVMinStr = "9.5"
)

// postgresGenericStat represent generic stat suitable for all kind of stats
type postgresGenericStat struct {
	labels map[string]string
	values map[string]float64
}

// parsePostgresGenericStats extracts labels and values from query result and returns stats object.
func parsePostgresGenericStats(r *model.PGResult, labelNames []string) map[string]postgresGenericStat {
	log.Debug("parse postgres generic stats")

	var stats = make(map[string]postgresGenericStat)

	// process row by row
	for _, row := range r.Rows {
		var stat = postgresGenericStat{
			labels: map[string]string{},
			values: map[string]float64{},
		}

		// collect label values and assemble map key
		var key string
		for i, colname := range r.Colnames {
			if stringsContains(labelNames, string(colname.Name)) {
				stat.labels[string(colname.Name)] = row[i].String
				key = key + "/" + row[i].String
			}
		}

		// trim leading slash
		key = strings.TrimLeft(key, "/")

		// Put stats with labels (but with no data values yet) into stats store.
		stats[key] = stat

		for i, colname := range r.Colnames {
			// Column's values act as metric values or as labels values.
			// If column's name is NOT in the labelNames, process column's values
			// as values for metrics. If column's name is in the labelNames, skip that column.
			if stringsContains(labelNames, string(colname.Name)) {
				log.Debugf("skip label mapped column '%s'", string(colname.Name))
				continue
			}

			// Skip empty (NULL) values.
			if !row[i].Valid {
				continue
			}

			// Get data value and convert it to float64 used by Prometheus.
			v, err := strconv.ParseFloat(row[i].String, 64)
			if err != nil {
				log.Errorf("invalid input, parse '%s' failed: %s; skip", row[i].String, err)
				continue
			}

			// Append value to values map
			stat.values[string(colname.Name)] = v
		}
	}

	return stats
}

// customValues unions values names and values in single place
type customValues map[string]float64

// postgresCustomStat unions customValues using label values as a key.
type postgresCustomStat map[string]customValues

// parsePostgresCustomStats parses query result, extract labels ans values and returns stats object.
func parsePostgresCustomStats(r *model.PGResult, labelNames []string) postgresCustomStat {
	log.Debug("parse postgres custom stats")

	stats := postgresCustomStat{}

	// process row by row
	for _, row := range r.Rows {
		// collect label values and assemble map key
		var key string
		for i, colname := range r.Colnames {
			if stringsContains(labelNames, string(colname.Name)) {
				key = key + "/" + row[i].String
			}
		}

		// trim leading slash
		key = strings.TrimLeft(key, "/")

		values := customValues{}
		for i, colname := range r.Colnames {
			// Column's values act as metric values or as labels values.
			// If column's name is NOT in the labelNames, process column's values as values for metrics. If column's name
			// is in the labelNames, skip that column.
			if stringsContains(labelNames, string(colname.Name)) {
				continue
			}

			// Skip empty (NULL) values.
			if !row[i].Valid {
				continue
			}

			// Convert value from string to float64.
			v, err := strconv.ParseFloat(row[i].String, 64)
			if err != nil {
				log.Errorf("invalid input, parse '%s' failed: %s; skip", row[i].String, err)
				continue
			}

			// Append value to values map
			values[string(colname.Name)] = v
		}

		stats[key] = values
	}

	return stats
}

// listDatabases returns slice with databases names
func listDatabases(db *store.DB) ([]string, error) {
	// getDBList returns the list of databases that allowed for connection
	rows, err := db.Conn().Query(context.Background(), "SELECT datname FROM pg_database WHERE NOT datistemplate AND datallowconn")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list = make([]string, 0, 10)
	for rows.Next() {
		var dbname string
		if err := rows.Scan(&dbname); err != nil {
			return nil, err
		}
		list = append(list, dbname)
	}
	return list, nil
}
