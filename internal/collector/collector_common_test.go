package collector

import (
	"database/sql"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"regexp"
	"strings"
	"sync"
	"testing"
)

func Test_newDeskSetsFromSubsystems(t *testing.T) {
	subsystems := map[string]model.MetricsSubsystem{
		// This should be in the output
		"example1": {
			Query: "SELECT 'label1' as label1, 1 as value1",
			Metrics: model.Metrics{
				{ShortName: "value1", Usage: "COUNTER", Labels: []string{"label1"}, Value: "value1", Description: "value1 description"},
			},
		},
		// This should be in the output
		"example2": {
			Databases: "example2",
			Query:     "SELECT 'label2' as label2, 2 as value2",
			Metrics: model.Metrics{
				{ShortName: "value2", Usage: "COUNTER", Labels: []string{"label2"}, Value: "value2", Description: "value2 description"},
			},
		},
	}

	constLabels := prometheus.Labels{"const": "constlabel"}

	subsysDescs := newDeskSetsFromSubsystems("example", subsystems, constLabels)
	assert.Equal(t, 2, len(subsysDescs))

	for _, set := range subsysDescs {
		assert.NotEqual(t, "", set.query)
		assert.NotNil(t, set.descs)
		assert.Equal(t, 1, len(subsysDescs[0].descs))
	}
}

func Test_newDescSet(t *testing.T) {
	subsys1 := model.MetricsSubsystem{
		Databases: "example",
		Query:     "SELECT 'l1' as label1, 'l21' as label2_1, 'l22' as label2_2, 100 as v1, 200 as v2",
		Metrics: model.Metrics{
			{ShortName: "metric1", Usage: "COUNTER", Labels: []string{"label1"}, Value: "v1", Description: "description"},
			{ShortName: "metric2", Usage: "COUNTER", Labels: []string{"label1"},
				LabeledValues: map[string][]string{"label2": {"label2_1", "label2_2"}}, Description: "description",
			},
		},
	}
	subsys2 := model.MetricsSubsystem{
		Query: "SELECT 'l1' as label1, 'l21' as label2_1, 'l22' as label2_2, 100 as v1, 200 as v2",
		Metrics: model.Metrics{
			{ShortName: "metric1", Usage: "COUNTER", Labels: []string{"label1"}, Value: "v1", Description: "description"},
			{ShortName: "metric2", Usage: "COUNTER", Labels: []string{"label1"},
				LabeledValues: map[string][]string{"label2": {"label2_1", "label2_2"}}, Description: "description",
			},
		},
	}

	desc, err := newDescSet("example", "test", subsys1, prometheus.Labels{"const": "constlabel"})
	assert.NoError(t, err)
	assert.NotNil(t, desc)
	assert.NotNil(t, desc.databasesRE)
	assert.Equal(t, "SELECT 'l1' as label1, 'l21' as label2_1, 'l22' as label2_2, 100 as v1, 200 as v2", desc.query)
	assert.Equal(t, 2, len(desc.descs))

	desc2, err := newDescSet("example", "test", subsys2, prometheus.Labels{"const": "constlabel"})
	assert.NoError(t, err)
	assert.NotNil(t, desc2)
	assert.Nil(t, desc2.databasesRE)
	assert.Equal(t, "SELECT 'l1' as label1, 'l21' as label2_1, 'l22' as label2_2, 100 as v1, 200 as v2", desc2.query)
	assert.Equal(t, 2, len(desc2.descs))
}

func Test_updateAllDescSets(t *testing.T) {
	config := Config{ConnString: store.TestPostgresConnStr}

	subsystems := map[string]model.MetricsSubsystem{
		// This should be in the output
		"example1": {
			Query: "SELECT 1 as value1",
			Metrics: model.Metrics{
				{ShortName: "value1", Usage: "COUNTER", Value: "value1", Description: "value1 description"},
			},
		},
		// This should be in the output
		"example2": {
			Query: "SELECT 'label2' as label2, 2 as value2",
			Metrics: model.Metrics{
				{ShortName: "value2", Usage: "COUNTER", Value: "value2", Description: "value2 description"},
			},
		},
		// This should be in the output
		"example3": {
			Databases: "pgscv_fixtures",
			Query:     "SELECT 'label3' as label3, 3 as value3",
			Metrics: model.Metrics{
				{ShortName: "value3", Usage: "COUNTER", Value: "value3", Description: "value3 description"},
			},
		},
		// This should be in the output
		"example4": {
			Databases: "pgscv_fixtures",
			Query:     "SELECT 4 as value4",
			Metrics: model.Metrics{
				{ShortName: "value4", Usage: "COUNTER", Value: "value4", Description: "value4 description"},
			},
		},
	}

	desksets := newDeskSetsFromSubsystems("postgres", subsystems, prometheus.Labels{"const": "example"})

	ch := make(chan prometheus.Metric)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		assert.NoError(t, updateAllDescSets(config, desksets, ch))
		close(ch)
		wg.Done()
	}()

	var counter = 0
	for m := range ch {
		//fmt.Println(m.Desc().String())
		counter++
		for _, s := range []string{"postgres_example", `const="example"`} {
			assert.True(t, strings.Contains(m.Desc().String(), s))
		}
	}
	assert.Equal(t, 4, counter)

	wg.Wait()
}

func Test_updateFromMultipleDatabases(t *testing.T) {
	config := Config{ConnString: store.TestPostgresConnStr}

	subsystems := map[string]model.MetricsSubsystem{
		// This should be skipped because it has no databases specified
		"example1": {
			Query: "SELECT 'label1' as label1, 1 as value1",
			Metrics: model.Metrics{
				{ShortName: "value1", Usage: "COUNTER", Value: "value1", Labels: []string{"label1"}, Description: "value1 description"},
			},
		},
		// This should be in the output
		"example2": {
			Databases: `pgscv_fixtures|invalid`,
			Query:     "SELECT 'label2' as label2, 'label3' as label3, 2 as value2",
			Metrics: model.Metrics{
				{ShortName: "value2", Usage: "COUNTER", Value: "value2", Labels: []string{"label2", "label3"}, Description: "value2 description"},
			},
		},
		"example3": {
			Databases: "pgscv_fixtures",
			Query:     "SELECT 3 as value3",
			Metrics: model.Metrics{
				{ShortName: "value3", Usage: "COUNTER", Value: "value3", Description: "value3 description"},
			},
		},
	}

	desksets := newDeskSetsFromSubsystems("postgres", subsystems, prometheus.Labels{"const": "example"})

	ch := make(chan prometheus.Metric)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		assert.NoError(t, updateFromMultipleDatabases(config, desksets, ch))
		close(ch)
		wg.Done()
	}()

	var counter = 0
	for range ch {
		//fmt.Println(m.Desc().String())
		counter++
	}
	assert.Equal(t, 2, counter)

	wg.Wait()
}

func Test_updateFromSingleDatabase(t *testing.T) {
	config := Config{ConnString: store.TestPostgresConnStr}

	subsystems := map[string]model.MetricsSubsystem{
		// This should be in the output
		"example1": {
			Query: "SELECT 'label1' as label1, 1 as value1",
			Metrics: model.Metrics{
				{ShortName: "value1", Usage: "COUNTER", Value: "value1", Labels: []string{"label1"}, Description: "value1 description"},
			},
		},
		// This should be skipped because it has databases specified
		"example2": {
			Databases: "pgscv_fixtures",
			Query:     "SELECT 'label2' as label2, 2 as value2",
			Metrics: model.Metrics{
				{ShortName: "value2", Usage: "COUNTER", Labels: []string{"label1"}, Value: "value2", Description: "value2 description"},
			},
		},
	}

	desksets := newDeskSetsFromSubsystems("postgres", subsystems, prometheus.Labels{"const": "example"})

	ch := make(chan prometheus.Metric)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		assert.NoError(t, updateFromSingleDatabase(config, desksets, ch))
		close(ch)
		wg.Done()
	}()

	var counter = 0
	for m := range ch {
		//fmt.Println(m.Desc().String())
		counter++
		for _, s := range []string{"postgres_example1_value1", `const="example"`, `variableLabels: [label1]`} {
			assert.True(t, strings.Contains(m.Desc().String(), s))
		}
	}
	assert.Equal(t, 1, counter)

	wg.Wait()
}

func Test_updateSingleDescSet(t *testing.T) {
	conn := store.NewTest(t)
	defer conn.Close()

	testcases := []struct {
		constLabels prometheus.Labels
		subsysName  string
		subsys      model.MetricsSubsystem
		want        []string
	}{
		{
			// descSet with no specified databases
			constLabels: prometheus.Labels{"constlabel": "example1"},
			subsysName:  "class1",
			subsys: model.MetricsSubsystem{
				Query: "SELECT 'l1' as label1, 0.123 as metric1, 0.456 as metric2",
				Metrics: model.Metrics{
					{ShortName: "label1", Usage: "LABEL", Description: "label1 description"},
					{ShortName: "metric1", Usage: "GAUGE", Description: "metric1 description"},
					{ShortName: "metric2", Usage: "GAUGE", Description: "metric2 description"},
				},
			},
			want: []string{"postgres_class1_metric", `constlabel="example1"`, `variableLabels: [label1]`},
		},
		{
			// descSet with specified databases
			constLabels: prometheus.Labels{"constlabel": "example2"},
			subsysName:  "class2",
			subsys: model.MetricsSubsystem{
				Databases: conn.Conn().Config().Database,
				Query:     "SELECT 'l1' as label1, 0.123 as metric1, 0.456 as metric2",
				Metrics: model.Metrics{
					{ShortName: "label1", Usage: "LABEL", Description: "label1 description"},
					{ShortName: "metric1", Usage: "GAUGE", Description: "metric1 description"},
					{ShortName: "metric2", Usage: "GAUGE", Description: "metric2 description"},
				},
			},
			want: []string{"postgres_class2_metric", `constlabel="example2"`, `variableLabels: [database label1]`},
		},
	}

	for i, tc := range testcases {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			set, err := newDescSet("postgres", tc.subsysName, tc.subsys, tc.constLabels)
			assert.NoError(t, err)
			ch := make(chan prometheus.Metric)

			var addDatabaseLabel bool
			if tc.subsys.Databases != "" {
				addDatabaseLabel = true
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				assert.NoError(t, updateSingleDescSet(conn, set, ch, addDatabaseLabel))
				close(ch)
				wg.Done()
			}()

			for m := range ch {
				//fmt.Println(m.Desc().String())
				for _, s := range tc.want {
					assert.True(t, strings.Contains(m.Desc().String(), s))
				}
			}

			wg.Wait()
		})
	}
}

func Test_updateMetrics(t *testing.T) {
	row := []sql.NullString{
		{String: "123", Valid: true}, {String: "987654", Valid: true}, // seq_scan, idx_scan
		{String: "852", Valid: true}, {String: "456", Valid: true}, {String: "753", Valid: true}, // ins, upd, del
		{String: "example", Valid: true},
	}
	colnames := []string{"seq_scan", "idx_scan", "inserted", "updated", "deleted", "relname"}

	testcases := []struct {
		desc         typedDesc
		dbLabelValue string
		want         int
	}{
		{
			desc: typedDesc{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "database", "tuples_total"),
					"description",
					[]string{"relname", "tuples"}, prometheus.Labels{"const": "example"},
				),
				valueType:     prometheus.CounterValue,
				value:         "",
				labeledValues: map[string][]string{"tuples": {"inserted", "updated", "deleted"}},
				labels:        []string{"relname", "tuples"},
			},
			dbLabelValue: "",
			want:         3,
		},
		{
			desc: typedDesc{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "table", "seq_scan_total"),
					"description",
					[]string{"database", "relname"}, prometheus.Labels{"const": "example"},
				),
				valueType:     prometheus.CounterValue,
				value:         "seq_scan",
				labeledValues: nil,
				labels:        []string{"database", "relname"},
			},
			dbLabelValue: "testdb",
			want:         1,
		},
	}

	for _, tc := range testcases {
		ch := make(chan prometheus.Metric)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			updateMetrics(row, tc.desc, colnames, ch, tc.dbLabelValue)
			close(ch)
			wg.Done()
		}()

		var counter int
		for range ch {
			counter++
			//fmt.Println(m.Desc().String())
		}

		assert.Equal(t, tc.want, counter)
		wg.Wait()
	}
}

func Test_updateMultipleMetrics(t *testing.T) {
	row := []sql.NullString{
		{String: "", Valid: false},                                                               // NULL value
		{String: "852", Valid: true}, {String: "456", Valid: true}, {String: "753", Valid: true}, // ins, upd, del
		{String: "example", Valid: true}, // relname
	}
	colnames := []string{"nullable", "inserted", "updated", "deleted", "relname"}

	testcases := []struct {
		desc         typedDesc
		dbLabelValue string
		want         int
	}{
		{
			desc: typedDesc{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "table", "tuples_total"),
					"description",
					[]string{"database", "relname", "tuples"}, prometheus.Labels{"const": "example"},
				),
				valueType:     prometheus.CounterValue,
				value:         "",
				labeledValues: map[string][]string{"tuples": {"inserted", "updated", "deleted"}},
				labels:        []string{"database", "relname", "tuples"},
			},
			dbLabelValue: "pgscv_fixtures",
			want:         3,
		},
		{
			desc: typedDesc{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "table", "tuples_total"),
					"description",
					[]string{"database", "tuples"}, prometheus.Labels{"const": "example"},
				),
				valueType:     prometheus.CounterValue,
				value:         "",
				labeledValues: map[string][]string{"tuples": {"inserted", "updated", "deleted"}},
				labels:        []string{"database", "tuples"},
			},
			dbLabelValue: "pgscv_fixtures",
			want:         3,
		},
		{
			// This is wrong case, but at at least it proves that no panic occurs
			desc: typedDesc{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "table", "tuples_total"),
					"description",
					nil, prometheus.Labels{"const": "example"},
				),
				valueType:     prometheus.CounterValue,
				value:         "",
				labeledValues: nil,
				labels:        nil,
			},
			dbLabelValue: "",
			want:         0,
		},
		{
			desc: typedDesc{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "table", "tuples_total"),
					"description",
					[]string{"database", "relname", "schema", "tuples"}, prometheus.Labels{"const": "example"},
				),
				valueType:     prometheus.CounterValue,
				value:         "",
				labeledValues: map[string][]string{"tuples": {"inserted", "updated", "deleted"}},
				labels:        []string{"database", "relname", "schema", "tuples"},
			},
			dbLabelValue: "pgscv_fixtures",
			want:         0,
		},
	}

	for _, tc := range testcases {
		ch := make(chan prometheus.Metric)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			updateMultipleMetrics(row, tc.desc, colnames, ch, tc.dbLabelValue)
			close(ch)
			wg.Done()
		}()

		var counter int
		for range ch {
			counter++
			//fmt.Println(m.Desc().String())
		}

		assert.Equal(t, tc.want, counter)
		wg.Wait()
	}
}

func Test_updateSingleMetric(t *testing.T) {
	row := []sql.NullString{
		{String: "123", Valid: true}, {String: "987654", Valid: true}, {String: "example", Valid: true}, {String: "", Valid: false},
	}
	colnames := []string{"seq_scan", "idx_scan", "relname", "nullable"}

	testcases := []struct {
		desc         typedDesc
		dbLabelValue string
		want         int
	}{
		{
			// many labels
			desc: typedDesc{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "table", "seq_scan_total"),
					"description",
					[]string{"database", "relname"}, prometheus.Labels{"const": "example"},
				),
				valueType:     prometheus.CounterValue,
				value:         "seq_scan",
				labeledValues: nil,
				labels:        []string{"database", "relname"},
			},
			dbLabelValue: "testdb",
			want:         1,
		},
		{
			// 'database' label is single in label list
			desc: typedDesc{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "table", "seq_scan_total"),
					"description",
					[]string{"database"}, prometheus.Labels{"const": "example"},
				),
				valueType:     prometheus.CounterValue,
				value:         "seq_scan",
				labeledValues: nil,
				labels:        []string{"database"},
			},
			dbLabelValue: "testdb",
			want:         1,
		},
		{
			// no labels
			desc: typedDesc{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "table", "seq_scan_total"),
					"description",
					nil, prometheus.Labels{"const": "example"},
				),
				valueType:     prometheus.CounterValue,
				value:         "seq_scan",
				labeledValues: nil,
				labels:        nil,
			},
			dbLabelValue: "",
			want:         1,
		},
		{
			// label which present in metric labels, but absent in data row.
			desc: typedDesc{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "table", "seq_scan_total"),
					"description",
					[]string{"database", "schemaname"}, prometheus.Labels{"const": "example"},
				),
				valueType:     prometheus.CounterValue,
				value:         "seq_scan",
				labeledValues: nil,
				labels:        []string{"database", "schemaname"},
			},
			dbLabelValue: "testdb",
			want:         0,
		},
	}

	for _, tc := range testcases {
		ch := make(chan prometheus.Metric)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			updateSingleMetric(row, tc.desc, colnames, ch, tc.dbLabelValue)
			close(ch)
			wg.Done()
		}()

		var counter int
		for range ch {
			counter++
			//fmt.Println(m.Desc().String())
		}

		assert.Equal(t, tc.want, counter)
		wg.Wait()
	}
}

func Test_needMultipleUpdate(t *testing.T) {
	testcases := []struct {
		sets []typedDescSet
		want bool
	}{
		{sets: []typedDescSet{{databasesRE: nil}}, want: false},
		{sets: []typedDescSet{{databasesRE: nil}, {databasesRE: nil}}, want: false},
		{sets: []typedDescSet{{databasesRE: regexp.MustCompile("example")}}, want: true},
		{
			sets: []typedDescSet{
				{databasesRE: nil},
				{databasesRE: regexp.MustCompile("example")},
			},
			want: true,
		},
		{
			sets: []typedDescSet{
				{databasesRE: nil},
				{databasesRE: regexp.MustCompile("example")},
				{databasesRE: nil},
			},
			want: true,
		},
	}

	for _, tc := range testcases {
		assert.Equal(t, tc.want, needMultipleUpdate(tc.sets))
	}
}

func Test_removeCollisions(t *testing.T) {
	s1 := model.Subsystems{
		"example1": {
			Query: "SELECT 'L1' AS label1, 1 AS value1",
			Metrics: model.Metrics{
				{ShortName: "value1", Usage: "COUNTER", Value: "value1", Labels: []string{"label1"}, Description: "description"},
			},
		},
	}
	s2 := model.Subsystems{
		"example1": {
			Query: "SELECT 'L1' AS label1, 1 AS value1",
			Metrics: model.Metrics{
				{ShortName: "value1", Usage: "COUNTER", Value: "value1", Labels: []string{"label1"}, Description: "description"},
			},
		},
		"example2": {
			Query: "SELECT 'L2' AS label2, 2 AS value2",
			Metrics: model.Metrics{
				{ShortName: "value2", Usage: "COUNTER", Value: "value1", Labels: []string{"label1"}, Description: "description"},
			},
		},
	}

	assert.Len(t, s1, 1)
	assert.Len(t, s2, 2)

	removeCollisions(s1, s2)

	assert.Len(t, s1, 1)
	assert.Len(t, s2, 1)
}
