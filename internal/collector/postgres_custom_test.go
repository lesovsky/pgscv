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

func TestPostgresCustomCollector_Update(t *testing.T) {
	settings := model.CollectorSettings{
		Subsystems: map[string]model.MetricsSubsystem{
			"example1": {
				Databases: []string{"pgscv_fixtures"},
				Query:     "SELECT 'label1' as l1, 1 as v1",
				Metrics: model.Metrics{
					{ShortName: "l1", Usage: "LABEL", Description: "l1 description"},
					{ShortName: "v1", Usage: "COUNTER", Description: "v1 description"},
				},
			},
			"example2": {
				Query: "SELECT 'label1' as l1, 'label2' as l2, 'label3' as l3, 1 as v1, 2 as v2",
				Metrics: model.Metrics{
					{ShortName: "l1", Usage: "LABEL", Description: "l1 description"},
					{ShortName: "l2", Usage: "LABEL", Description: "l2 description"},
					{ShortName: "l3", Usage: "LABEL", Description: "l3 description"},
					{ShortName: "v1", Usage: "COUNTER", Description: "v1 description"},
					{ShortName: "v2", Usage: "GAUGE", Description: "v2 description"},
				},
			},
		},
	}

	var input = pipelineInput{
		required: []string{
			"postgres_example1_v1",
			"postgres_example2_v1",
			"postgres_example2_v2",
		},
		collector:         NewPostgresCustomCollector,
		collectorSettings: settings,
		service:           model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_postgresCustomCollector_updateFromMultipleDatabases(t *testing.T) {
	config := Config{ConnString: store.TestPostgresConnStr}
	databases := []string{"pgscv_fixtures", "postgres", "invalid"}

	settings := model.CollectorSettings{
		Subsystems: map[string]model.MetricsSubsystem{
			"example1": {
				Databases: []string{"pgscv_fixtures"},
				Query:     "SELECT 'label1' as l1, 1 as v1",
				Metrics: model.Metrics{
					{ShortName: "l1", Usage: "LABEL", Description: "l1 description"},
					{ShortName: "v1", Usage: "COUNTER", Description: "v1 description"},
				},
			},
			"example2": {
				Query: "SELECT 'label1' as l1, 'label2' as l2, 'label3' as l3, 1 as v1, 2 as v2",
				Metrics: model.Metrics{
					{ShortName: "l1", Usage: "LABEL", Description: "l1 description"},
					{ShortName: "l2", Usage: "LABEL", Description: "l2 description"},
					{ShortName: "l3", Usage: "LABEL", Description: "l3 description"},
					{ShortName: "v1", Usage: "COUNTER", Description: "v1 description"},
					{ShortName: "v2", Usage: "GAUGE", Description: "v2 description"},
				},
			},
		},
	}

	collector, err := NewPostgresCustomCollector(prometheus.Labels{"constlabel": "example1"}, settings)
	assert.NoError(t, err)

	ch := make(chan prometheus.Metric)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		assert.NoError(t, collector.(*postgresCustomCollector).updateFromMultipleDatabases(config, databases, ch))
		close(ch)
		wg.Done()
	}()

	for m := range ch {
		//fmt.Println(m.Desc().String())
		for _, s := range []string{"postgres_example1_v1", `constlabel="example1"`, `variableLabels: [database l1]`} {
			assert.True(t, strings.Contains(m.Desc().String(), s))
		}
	}

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
