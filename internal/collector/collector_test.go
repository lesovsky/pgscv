package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPgscvCollector_Collect(t *testing.T) {
	// Create test stuff - factory and collector, register system only metrics.
	f := Factories{}
	f.RegisterSystemCollectors([]string{})
	c, err := NewPgscvCollector("test:0", f, Config{})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	// Create channel and run Collect method which collect metrics and transmit them into channel.
	ch := make(chan prometheus.Metric)

	go func() {
		c.Collect(ch)
		close(ch)
	}()

	// Catch metrics until channel is opened.
	var metrics []prometheus.Metric
	for m := range ch {
		//metric := &io_prometheus_client.Metric{}
		//_ = m.Write(metric)
		//fmt.Println("debug: ", proto.MarshalTextString(metric))

		metrics = append(metrics, m)
	}

	// Check metrics slice should not be nil or empty.
	assert.NotNil(t, metrics)
	assert.Greater(t, len(metrics), 0)
}
