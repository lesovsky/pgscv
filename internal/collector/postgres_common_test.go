package collector

import (
	"database/sql"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/jackc/pgproto3/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_parseStats(t *testing.T) {
	var testDescs = []typedDesc{
		{
			colname: "xact_commit",
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("pgscv", "database", "xact_commit_total"),
				"The total number of transactions committed.",
				[]string{"datname"}, prometheus.Labels{"example_label": "example_value"},
			), valueType: prometheus.CounterValue,
		},
		{
			colname: "xact_rollback",
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("pgscv", "database", "xact_rollback_total"),
				"The total number of transactions rolled back.",
				[]string{"datname"}, prometheus.Labels{"example_label": "example_value"},
			), valueType: prometheus.CounterValue,
		},
	}

	var testCases = []struct {
		name         string
		metricsTotal int
		res          *store.QueryResult
	}{
		{
			name:         "1 valid query result",
			metricsTotal: 4,
			res: &store.QueryResult{
				Nrows:    2,
				Ncols:    3,
				Colnames: []pgproto3.FieldDescription{{Name: []byte("datname")}, {Name: []byte("xact_commit")}, {Name: []byte("xact_rollback")}},
				Rows: [][]sql.NullString{
					{{String: "example1", Valid: true}, {String: "100", Valid: true}, {String: "1000", Valid: true}},
					{{String: "example2", Valid: true}, {String: "200", Valid: true}, {String: "2000", Valid: true}},
				},
			},
		},
		{
			name:         "2 valid query result, with empty (NULL) value",
			metricsTotal: 2,
			res: &store.QueryResult{
				Nrows:    1,
				Ncols:    3,
				Colnames: []pgproto3.FieldDescription{{Name: []byte("datname")}, {Name: []byte("xact_commit")}, {Name: []byte("xact_rollback")}},
				Rows: [][]sql.NullString{
					{{String: "example3", Valid: true}, {String: "200", Valid: true}, {String: "", Valid: false}},
				},
			},
		},
		{
			name:         "3 invalid (text) value",
			metricsTotal: 1,
			res: &store.QueryResult{
				Nrows:    1,
				Ncols:    3,
				Colnames: []pgproto3.FieldDescription{{Name: []byte("datname")}, {Name: []byte("xact_commit")}, {Name: []byte("xact_rollback")}},
				Rows: [][]sql.NullString{
					{{String: "example3", Valid: true}, {String: "200", Valid: true}, {String: "invalid", Valid: false}},
				},
			},
		},
		{
			name:         "4 unknown column in result set",
			metricsTotal: 1,
			res: &store.QueryResult{
				Nrows:    1,
				Ncols:    3,
				Colnames: []pgproto3.FieldDescription{{Name: []byte("datname")}, {Name: []byte("xact_commit")}, {Name: []byte("unknown")}},
				Rows: [][]sql.NullString{
					{{String: "example3", Valid: true}, {String: "200", Valid: true}, {String: "300", Valid: true}},
				},
			},
		},
	}

	for i, tc := range testCases {
		var ch = make(chan prometheus.Metric)
		tcCopy := tc
		go func() {
			assert.NoError(t, parseStats(tcCopy.res, ch, testDescs, []string{"datname"}))
			close(ch)
		}()

		for metric := range ch {
			switch i {
			case 1:
				assert.Regexp(t, "pgscv_database_xact_commit_total|pgscv_database_xact_rollback_total", metric.Desc().String())
			case 2:
				assert.Regexp(t, "pgscv_database_xact_commit_total|pgscv_database_xact_rollback_total", metric.Desc().String())
			case 3:
				assert.Regexp(t, "pgscv_database_xact_commit_total", metric.Desc().String())
				assert.NotRegexp(t, "pgscv_database_xact_rollback_total", metric.Desc().String())
			case 4:
				assert.Regexp(t, "pgscv_database_xact_commit_total", metric.Desc().String())
				assert.NotRegexp(t, "pgscv_database_xact_rollback_total", metric.Desc().String())
			}
			log.Infoln(metric.Desc().String())
		}
	}
}
