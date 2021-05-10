package collector

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"strings"
	"sync"
	"testing"
)

func Test_newDescSet(t *testing.T) {
	testcases := []struct {
		subsystem       model.MetricsSubsystem
		wantQuery       string
		wantVarLabels   []string
		wantMetricNames []string
	}{
		{
			// With no databases specified
			subsystem: model.MetricsSubsystem{
				Query: "SELECT 'l1' as label1, 'l2' as label2, 0.123 as metric1, 0.456 as metric2, 0.789 as metric3",
				Metrics: model.Metrics{
					{ShortName: "label1", Usage: "LABEL", Description: "label1 description"},
					{ShortName: "label2", Usage: "LABEL", Description: "label2 description"},
					{ShortName: "metric1", Usage: "GAUGE", Description: "metric1 description"},
					{ShortName: "metric2", Usage: "GAUGE", Description: "metric2 description"},
					{ShortName: "metric3", Usage: "GAUGE", Description: "metric3 description"},
				},
			},
			wantQuery:       "SELECT 'l1' as label1, 'l2' as label2, 0.123 as metric1, 0.456 as metric2, 0.789 as metric3",
			wantVarLabels:   []string{"label1", "label2"},
			wantMetricNames: []string{"metric1", "metric2", "metric3"},
		},
		{
			// With databases specified
			subsystem: model.MetricsSubsystem{
				Databases: []string{"pgscv_fixtures"},
				Query:     "SELECT 'l1' as label1, 'l2' as label2, 0.123 as metric1, 0.456 as metric2, 0.789 as metric3",
				Metrics: model.Metrics{
					{ShortName: "label1", Usage: "LABEL", Description: "label1 description"},
					{ShortName: "label2", Usage: "LABEL", Description: "label2 description"},
					{ShortName: "metric1", Usage: "GAUGE", Description: "metric1 description"},
					{ShortName: "metric2", Usage: "GAUGE", Description: "metric2 description"},
					{ShortName: "metric3", Usage: "GAUGE", Description: "metric3 description"},
				},
			},
			wantQuery:       "SELECT 'l1' as label1, 'l2' as label2, 0.123 as metric1, 0.456 as metric2, 0.789 as metric3",
			wantVarLabels:   []string{"database", "label1", "label2"},
			wantMetricNames: []string{"metric1", "metric2", "metric3"},
		},
	}

	constLabels := prometheus.Labels{"constlabel": "example"}

	for _, tc := range testcases {
		descSet := newDescSet(constLabels, "postgres", "class", tc.subsystem)
		assert.Equal(t, tc.wantQuery, descSet.query)
		assert.Equal(t, tc.wantVarLabels, descSet.variableLabels)
		assert.Equal(t, tc.wantMetricNames, descSet.metricNames)
		assert.NotNil(t, descSet.descs)
	}
}

func Test_newDeskSetsFromSubsystems(t *testing.T) {
	subsystems := map[string]model.MetricsSubsystem{
		// This should be in the output
		"example1": {
			Query: "SELECT 'label1' as label1, 1 as value1",
			Metrics: model.Metrics{
				{ShortName: "label1", Usage: "LABEL", Description: "label1 description"},
				{ShortName: "value1", Usage: "COUNTER", Description: "value1 description"},
			},
		},
		// This should be in the output
		"example2": {
			Databases: []string{"example2"},
			Query:     "SELECT 'label2' as label2, 2 as value2",
			Metrics: model.Metrics{
				{ShortName: "label2", Usage: "LABEL", Description: "label2 description"},
				{ShortName: "value2", Usage: "COUNTER", Description: "value2 description"},
			},
		},
	}

	desksets := newDeskSetsFromSubsystems("postgres", subsystems, prometheus.Labels{"const": "example"})
	assert.Equal(t, 2, len(desksets))

	for _, set := range desksets {
		assert.NotEqual(t, "", set.query)
		assert.NotNil(t, set.variableLabels)
		assert.NotNil(t, set.metricNames)
		assert.Equal(t, 1, len(desksets[0].descs))
	}
}

func Test_UpdateDescSet(t *testing.T) {
	config := Config{ConnString: store.TestPostgresConnStr}

	subsystems := map[string]model.MetricsSubsystem{
		// This should be in the output
		"example1": {
			Query: "SELECT 1 as value1",
			Metrics: model.Metrics{
				{ShortName: "value1", Usage: "COUNTER", Description: "value1 description"},
			},
		},
		// This should be in the output
		"example2": {
			Query: "SELECT 'label2' as label2, 2 as value2",
			Metrics: model.Metrics{
				{ShortName: "label2", Usage: "LABEL", Description: "label2 description"},
				{ShortName: "value2", Usage: "COUNTER", Description: "value2 description"},
			},
		},
		// This should be in the output
		"example3": {
			Databases: []string{"pgscv_fixtures"},
			Query:     "SELECT 'label3' as label3, 3 as value3",
			Metrics: model.Metrics{
				{ShortName: "label3", Usage: "LABEL", Description: "label3 description"},
				{ShortName: "value3", Usage: "COUNTER", Description: "value3 description"},
			},
		},
		// This should be in the output
		"example4": {
			Databases: []string{"pgscv_fixtures"},
			Query:     "SELECT 4 as value4",
			Metrics: model.Metrics{
				{ShortName: "value4", Usage: "COUNTER", Description: "value4 description"},
			},
		},
	}

	desksets := newDeskSetsFromSubsystems("postgres", subsystems, prometheus.Labels{"const": "example"})

	ch := make(chan prometheus.Metric)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		assert.NoError(t, UpdateDescSet(config, desksets, ch))
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

func Test_updateFromSingleDatabase(t *testing.T) {
	config := Config{ConnString: store.TestPostgresConnStr}

	subsystems := map[string]model.MetricsSubsystem{
		// This should be in the output
		"example1": {
			Query: "SELECT 'label1' as label1, 1 as value1",
			Metrics: model.Metrics{
				{ShortName: "label1", Usage: "LABEL", Description: "label1 description"},
				{ShortName: "value1", Usage: "COUNTER", Description: "value1 description"},
			},
		},
		// This should be skipped because it has databases specified
		"example2": {
			Databases: []string{"pgscv_fixtures"},
			Query:     "SELECT 'label2' as label2, 2 as value2",
			Metrics: model.Metrics{
				{ShortName: "label2", Usage: "LABEL", Description: "label2 description"},
				{ShortName: "value2", Usage: "COUNTER", Description: "value2 description"},
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

func Test_updateFromMultipleDatabases(t *testing.T) {
	config := Config{ConnString: store.TestPostgresConnStr}
	databases := []string{"pgscv_fixtures", "postgres", "invalid"}

	subsystems := map[string]model.MetricsSubsystem{
		// This should be skipped because it has no databases specified
		"example1": {
			Query: "SELECT 'label1' as label1, 1 as value1",
			Metrics: model.Metrics{
				{ShortName: "label1", Usage: "LABEL", Description: "label1 description"},
				{ShortName: "value1", Usage: "COUNTER", Description: "value1 description"},
			},
		},
		// This should be in the output
		"example2": {
			Databases: []string{"pgscv_fixtures", "invalid"},
			Query:     "SELECT 'label2' as label2, 'label3' as label3, 2 as value2",
			Metrics: model.Metrics{
				{ShortName: "label2", Usage: "LABEL", Description: "label2 description"},
				{ShortName: "label3", Usage: "LABEL", Description: "label3 description"},
				{ShortName: "value2", Usage: "COUNTER", Description: "value2 description"},
			},
		},
		"example3": {
			Databases: []string{"pgscv_fixtures"},
			Query:     "SELECT 3 as value3",
			Metrics: model.Metrics{
				{ShortName: "value3", Usage: "COUNTER", Description: "value3 description"},
			},
		},
	}

	desksets := newDeskSetsFromSubsystems("postgres", subsystems, prometheus.Labels{"const": "example"})

	ch := make(chan prometheus.Metric)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		assert.NoError(t, updateFromMultipleDatabases(config, desksets, databases, ch))
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

func Test_updateDescSet(t *testing.T) {
	conn := store.NewTest(t)
	defer conn.Close()

	testcases := []struct {
		set  typedDescSet
		want []string
	}{
		{
			// descSet with no specified databases
			set: newDescSet(
				prometheus.Labels{"constlabel": "example1"}, "postgres", "class1",
				model.MetricsSubsystem{
					Query: "SELECT 'l1' as label1, 0.123 as metric1, 0.456 as metric2",
					Metrics: model.Metrics{
						{ShortName: "label1", Usage: "LABEL", Description: "label1 description"},
						{ShortName: "metric1", Usage: "GAUGE", Description: "metric1 description"},
						{ShortName: "metric2", Usage: "GAUGE", Description: "metric2 description"},
					},
				},
			),
			want: []string{"postgres_class1_metric", `constlabel="example1"`, `variableLabels: [label1]`},
		},
		{
			// descSet with specified databases
			set: newDescSet(
				prometheus.Labels{"constlabel": "example2"}, "postgres", "class2",
				model.MetricsSubsystem{
					Databases: []string{conn.Conn().Config().Database},
					Query:     "SELECT 'l1' as label1, 0.123 as metric1, 0.456 as metric2",
					Metrics: model.Metrics{
						{ShortName: "label1", Usage: "LABEL", Description: "label1 description"},
						{ShortName: "metric1", Usage: "GAUGE", Description: "metric1 description"},
						{ShortName: "metric2", Usage: "GAUGE", Description: "metric2 description"},
					},
				},
			),
			want: []string{"postgres_class2_metric", `constlabel="example2"`, `variableLabels: [database label1]`},
		},
	}

	for i, tc := range testcases {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			ch := make(chan prometheus.Metric)

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				assert.NoError(t, updateDescSet(conn, tc.set, ch))
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

func Test_listDeskSetDatabases(t *testing.T) {
	testcases := []struct {
		sets []typedDescSet
		want int
	}{
		{sets: []typedDescSet{{databases: []string{}}}, want: 0},
		{sets: []typedDescSet{{databases: []string{"example1"}}}, want: 1},
		{sets: []typedDescSet{{databases: []string{"example1", "example2"}}}, want: 2},
		{
			sets: []typedDescSet{
				{databases: []string{"example1", "example2"}},
				{databases: []string{"example2", "example3"}},
			},
			want: 3,
		},
		{
			sets: []typedDescSet{
				{databases: []string{"example1", "example2"}},
				{databases: []string{"example2", "example3"}},
				{databases: []string{"example3", "example1"}},
			},
			want: 3,
		},
	}

	for _, tc := range testcases {
		assert.Equal(t, tc.want, len(listDeskSetDatabases(tc.sets)))
	}
}

func Test_removeCollisions(t *testing.T) {
	s1 := model.Subsystems{
		"example1": {
			Query: "SELECT 'label1' AS label1, 1 AS value1",
			Metrics: model.Metrics{
				{ShortName: "label1", Usage: "LABEL", Description: "label1 description."},
				{ShortName: "value1", Usage: "COUNTER", Description: "value1 description."},
			},
		},
	}
	s2 := model.Subsystems{
		"example1": {
			Query: "SELECT 'label1' AS label1, 2 AS value2",
			Metrics: model.Metrics{
				{ShortName: "label1", Usage: "LABEL", Description: "label1 description."},
				{ShortName: "value2", Usage: "COUNTER", Description: "value2 description."},
			},
		},
		"example2": {
			Query: "SELECT 'label1' AS label1, 1 AS value1",
			Metrics: model.Metrics{
				{ShortName: "label1", Usage: "LABEL", Description: "label1 description."},
				{ShortName: "value1", Usage: "COUNTER", Description: "value1 description."},
			},
		},
	}

	assert.Len(t, s1, 1)
	assert.Len(t, s2, 2)

	removeCollisions(s1, s2)

	assert.Len(t, s1, 1)
	assert.Len(t, s2, 1)
}
