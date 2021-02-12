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

// parsePostgresGenericStats extracts values from query result, generates metrics using extracted values and passed
// labels and send them to Prometheus.
func parsePostgresGenericStats(r *model.PGResult, labelNames []string) map[string]postgresGenericStat {
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
			// If column's name is NOT in the labelNames, process column's values as values for metrics. If column's name
			// is in the labelNames, skip that column.
			if stringsContains(labelNames, string(colname.Name)) {
				log.Debug("skip label mapped column")
				continue
			}

			// Skip empty (NULL) values.
			if row[i].String == "" {
				log.Debug("got empty (NULL) value, skip")
				continue
			}

			// Get data value and convert it to float64 used by Prometheus.
			v, err := strconv.ParseFloat(row[i].String, 64)
			if err != nil {
				log.Errorf("skip collecting metric: %s", err)
				continue
			}

			// Append value to values map
			stat.values[string(colname.Name)] = v
		}
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

// isExtensionAvailable returns true if extension with specified name exists and available
func isExtensionAvailable(db *store.DB, name string) bool {
	log.Debugf("check %s availability", name)

	var exists bool
	err := db.Conn().
		QueryRow(context.Background(), "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = $1)", name).
		Scan(&exists)
	if err != nil {
		log.Errorln("failed to check extensions in pg_extension: ", err)
		return false
	}

	// Return false if extension is not installed.
	return exists
}
