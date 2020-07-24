package collector

import (
	"database/sql"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
	"strings"
)

const poolQuery = "SHOW POOLS"

type pgbouncerPoolsCollector struct {
	descs      []typedDesc
	labelNames []string
	connStats  map[string]connStat
	conns      *prometheus.Desc
}

type connStat struct {
	clActive  float64
	clWaiting float64
	svActive  float64
	svIdle    float64
	svUsed    float64
	svTested  float64
	svLogin   float64
}

func NewPgbouncerPoolsCollector(constLabels prometheus.Labels) (Collector, error) {
	var poolsLabelNames = []string{"database", "user", "pool_mode", "state"}

	return &pgbouncerPoolsCollector{
		conns: prometheus.NewDesc(
			prometheus.BuildFQName("pgscv", "pgbouncer", "pool_conn_total"),
			"The total number of connections established.",
			poolsLabelNames, constLabels,
		),
		labelNames: poolsLabelNames,
		//descs: []typedDesc{
		//	{
		//		colname: "cl_active",
		//		desc: prometheus.NewDesc(
		//			prometheus.BuildFQName("pgscv", "pgbouncer", "pool_cl_active_total"),
		//			"The total number of active clients connected.",
		//			poolsLabelNames, constLabels,
		//		), valueType: prometheus.GaugeValue,
		//	},
		//	{
		//		colname: "cl_waiting",
		//		desc: prometheus.NewDesc(
		//			prometheus.BuildFQName("pgscv", "pgbouncer", "pool_cl_waiting_total"),
		//			"The total number of waiting clients connected.",
		//			poolsLabelNames, constLabels,
		//		), valueType: prometheus.GaugeValue,
		//	},
		//},
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *pgbouncerPoolsCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	log.Infoln("lessqq 1: start update")
	conn, err := store.NewDB(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	res, err := conn.GetStats(poolQuery)
	if err != nil {
		return err
	}

	stats := parseStatsExtended(res, c.labelNames)
	log.Infoln("lessqq 2: ", len(stats))

	//ch <- prometheus.MustNewConstMetric(descs[idx].desc, descs[idx].valueType, v, labelValues...)
	for poolname, poolstat := range stats {
		log.Infoln("lessqq: ", poolname)
		props := strings.Split(poolname, "/")
		if len(props) != 3 {
			log.Warnf("incomplete poolname: %s; skip", poolname)
			continue
		}
		ch <- prometheus.MustNewConstMetric(c.conns, prometheus.GaugeValue, poolstat.clActive, props[0], props[1], props[2], "cl_active")
	}

	return nil
}

func parseStatsExtended(r *store.QueryResult, labelNames []string) map[string]connStat {
	var stats = map[string]connStat{}
	var poolname string

	type poolProperties struct {
		database string
		user     string
		mode     string
	}

	for _, row := range r.Rows {
		props := poolProperties{}
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "database":
				props.database = row[i].String
			case "user":
				props.user = row[i].String
			case "pool_mode":
				props.mode = row[i].String
			}
		}

		// create a poolname consisting of trio database/user/pool_mode
		poolname = strings.Join([]string{props.database, props.user, props.mode}, "/")
		log.Infoln("lessqq 3 poolname: ", poolname)

		for i, colname := range r.Colnames {
			// Column's values act as metric values or as labels values.
			// If column's name is NOT in the labelNames, process column's values as values for metrics. If column's name
			// is in the labelNames, skip that column.
			if !stringsContains(labelNames, string(colname.Name)) {
				//var labelValues = make([]string, len(labelNames))

				// Get values from columns which are specified in labelNames. These values will be attached to the metric.
				//for j, lname := range labelNames {
				//  // Get the index of the column in QueryResult, using that index fetch the value from row's values.
				//  for idx, cname := range r.Colnames {
				//    if lname == string(cname.Name) {
				//      labelValues[j] = row[idx].String
				//    }
				//  }
				//}

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
				//idx, err := lookupByColname(descs, string(colname.Name))
				//if err != nil {
				//  log.Warnf("skip collecting metric: %s", err)
				//  continue
				//}

				switch string(colname.Name) {
				case "cl_active":
					s := stats[poolname]
					s.clActive = v
					stats[poolname] = s
				case "cl_waiting":
					s := stats[poolname]
					s.clActive = v
					stats[poolname] = s
				}

				// Generate metric and throw it to Prometheus.
				//ch <- prometheus.MustNewConstMetric(descs[idx].desc, descs[idx].valueType, v, labelValues...)
			}
		}
	}

	return stats
}
