package pgscv

import (
	"context"
	"github.com/lesovsky/pgscv/internal/http"
	"github.com/stretchr/testify/assert"
	"io"
	"sync"
	"testing"
	"time"
)

func TestStart(t *testing.T) {
	writeSrv := http.TestServer(t, http.StatusOK, "")
	defer writeSrv.Close()

	// Create app config.
	config := &Config{
		ListenAddress: "127.0.0.1:5002",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Start app, wait until context expires and do cleanup.
	assert.NoError(t, Start(ctx, config))
}

func Test_runMetricsListener(t *testing.T) {
	config := &Config{ListenAddress: "127.0.0.1:5003"}
	wg := sync.WaitGroup{}

	// Running listener function with short-live context in concurrent goroutine.
	wg.Add(1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		err := runMetricsListener(ctx, config)
		assert.NoError(t, err)
		wg.Done()
	}()

	// Sleep little bit hoping it will be enough for running listener goroutine.
	time.Sleep(500 * time.Millisecond)

	// Make request to '/' and assert response.
	cl := http.NewClient(http.ClientConfig{})
	resp, err := cl.Get("http://127.0.0.1:5003/")
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Contains(t, string(body), `pgSCV / PostgreSQL metrics collector, for more info visit <a href="https://github.com/lesovsky/pgscv">Github</a> page.`)
	assert.NoError(t, resp.Body.Close())

	// Make request to '/metrics' and assert response.
	resp, err = cl.Get("http://127.0.0.1:5003/metrics")
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode, http.StatusOK)

	body, err = io.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Contains(t, string(body), "go_gc_duration_seconds")
	assert.Contains(t, string(body), "process_cpu_seconds_total")
	assert.Contains(t, string(body), "promhttp_metric_handler_requests_in_flight")
	assert.NoError(t, resp.Body.Close())

	// Waiting for listener goroutine.
	wg.Wait()
}
