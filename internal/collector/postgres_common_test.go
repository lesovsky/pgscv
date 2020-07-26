package collector

import (
	"database/sql"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/jackc/pgproto3/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"regexp"
	"testing"
)

func Test_parsePostgresStats(t *testing.T) {
	var testDescs = []typedDesc{
		{
			colname: "test_alfa",
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("test", "example", "alfa_total"),
				"Test example alfa.",
				[]string{"testname"}, prometheus.Labels{"example_label": "example_value"},
			), valueType: prometheus.CounterValue,
		},
		{
			colname: "test_beta",
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("test", "example", "beta_total"),
				"Test example beta.",
				[]string{"testname"}, prometheus.Labels{"example_label": "example_value"},
			), valueType: prometheus.CounterValue,
		},
	}

	var testCases = []struct {
		name         string
		metricsTotal int
		res          *store.QueryResult
		required     []string
	}{
		{
			name:         "1 valid query result",
			metricsTotal: 4,
			res: &store.QueryResult{
				Nrows:    2,
				Ncols:    3,
				Colnames: []pgproto3.FieldDescription{{Name: []byte("testname")}, {Name: []byte("test_alfa")}, {Name: []byte("test_beta")}},
				Rows: [][]sql.NullString{
					{{String: "random1", Valid: true}, {String: "100", Valid: true}, {String: "1000", Valid: true}},
					{{String: "random2", Valid: true}, {String: "200", Valid: true}, {String: "2000", Valid: true}},
				},
			},
			required: []string{"test_example_alfa_total", "test_example_beta_total"},
		},
		{
			name:         "2 valid query result, with empty (NULL) value",
			metricsTotal: 2,
			res: &store.QueryResult{
				Nrows:    1,
				Ncols:    3,
				Colnames: []pgproto3.FieldDescription{{Name: []byte("testname")}, {Name: []byte("test_alfa")}, {Name: []byte("test_beta")}},
				Rows: [][]sql.NullString{
					{{String: "random3", Valid: true}, {String: "200", Valid: true}, {String: "", Valid: false}},
				},
			},
			required: []string{"test_example_alfa_total"},
		},
		{
			name:         "3 invalid (text) value",
			metricsTotal: 1,
			res: &store.QueryResult{
				Nrows:    1,
				Ncols:    3,
				Colnames: []pgproto3.FieldDescription{{Name: []byte("testname")}, {Name: []byte("test_alfa")}, {Name: []byte("test_beta")}},
				Rows: [][]sql.NullString{
					{{String: "random4", Valid: true}, {String: "200", Valid: true}, {String: "invalid", Valid: false}},
				},
			},
			required: []string{"test_example_alfa_total"},
		},
		{
			name:         "4 unknown column in result set",
			metricsTotal: 1,
			res: &store.QueryResult{
				Nrows:    1,
				Ncols:    3,
				Colnames: []pgproto3.FieldDescription{{Name: []byte("testname")}, {Name: []byte("test_alfa")}, {Name: []byte("unknown")}},
				Rows: [][]sql.NullString{
					{{String: "random5", Valid: true}, {String: "200", Valid: true}, {String: "300", Valid: true}},
				},
			},
			required: []string{"test_example_alfa_total"},
		},
	}

	for _, tc := range testCases {
		var ch = make(chan prometheus.Metric)
		tcCopy := tc
		go func() {
			assert.NoError(t, parsePostgresStats(tcCopy.res, ch, testDescs, []string{"testname"}))
			close(ch)
		}()

		metricNamesCounter := map[string]int{}
		// test all required metrics are generated
		for metric := range ch {
			re := regexp.MustCompile(`fqName: "([a-z_]+)"`)
			match := re.FindStringSubmatch(metric.Desc().String())[1]
			assert.Contains(t, tc.required, match)
			metricNamesCounter[match] += 1
		}

		// check there are no metrics generated other than required
		for _, s := range tc.required {
			if v, ok := metricNamesCounter[s]; !ok {
				assert.Fail(t, "necessary metric not found in the map: ", s)
			} else {
				assert.Greater(t, v, 0)
			}
		}
	}
}
