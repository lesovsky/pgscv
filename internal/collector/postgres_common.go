package collector

import (
	"database/sql"
	"fmt"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
)

type typedDesc struct {
	// name is the name of column in a query output used for getting value for metric.
	colname string
	// desc is the descriptor used by every Prometheus Metric.
	desc *prometheus.Desc
	// valueType is an enumeration of metric types that represent a simple value.
	valueType prometheus.ValueType
}

// lookupDesc returns index of the descriptor with colname specified in pattern
func lookupByColname(descs []typedDesc, pattern string) (int, error) {
	for i, desc := range descs {
		if desc.colname == pattern {
			return i, nil
		}
	}
	return -1, fmt.Errorf("pattern not found")
}

// parseStats extracts values from query result, generates metrics using extracted values and passed
// labels and send them to Prometheus.
func parseStats(r *store.QueryResult, ch chan<- prometheus.Metric, descs []typedDesc, labelNames []string) error {
	for _, row := range r.Rows {
		for i, colname := range r.Colnames {
			// Column's values act as metric values or as labels values.
			// If column's name is NOT in the labelNames, process column's values as values for metrics. If column's name
			// is in the labelNames, skip that column.
			if !stringsContains(labelNames, string(colname.Name)) {
				var labelValues = make([]string, len(labelNames))

				// Get values from columns which are specified in labelNames. These values will be attached to the metric.
				for j, lname := range labelNames {
					// Get the index of the column in QueryResult, using that index fetch the value from row's values.
					for idx, cname := range r.Colnames {
						if lname == string(cname.Name) {
							labelValues[j] = row[idx].String
						}
					}
				}

				// Empty (NULL) values are converted to zeros.
				if row[i].String == "" {
					log.Debugf("got empty value, convert it to zero")
					row[i] = sql.NullString{String: "0", Valid: true}
				}

				// Get data value and convert it to float64 used by Prometheus.
				v, err := strconv.ParseFloat(row[i].String, 64)
				if err != nil {
					log.Warnf("skip collecting metric: %s", err)
					continue
				}

				// Get index of the descriptor from 'descs' slice using column's name. This index will be needed below when need
				// to tie up extracted data values with suitable metric descriptor - column's name here is the key.
				idx, err := lookupByColname(descs, string(colname.Name))
				if err != nil {
					log.Warnf("skip collecting metric: %s", err)
					continue
				}

				// Generate metric and throw it to Prometheus.
				ch <- prometheus.MustNewConstMetric(descs[idx].desc, descs[idx].valueType, v, labelValues...)
			}
		}
	}

	return nil
}
