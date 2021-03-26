package pgscv

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/weaponry/pgscv/internal/service"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestStart(t *testing.T) {
	// Mock HTTP server which handles incoming requests.
	writeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.Greater(t, len(body), 0)
		assert.NoError(t, r.Body.Close())

		w.WriteHeader(http.StatusOK)
	}))
	defer writeServer.Close()

	// Create app config.
	config := &Config{
		ListenAddress:  "127.0.0.1:5002",
		APIKey:         "TEST1234TEST-TEST-1234-TEST1234",
		SendMetricsURL: writeServer.URL, SendMetricsInterval: 1 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Start app, wait until context expires and do cleanup.
	assert.NoError(t, Start(ctx, config))
	http.DefaultServeMux = new(http.ServeMux)
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
	resp, err := http.Get("http://127.0.0.1:5003/")
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Contains(t, string(body), "pgSCV / Weaponry metric collector, for more info visit https://github.com/weaponry/pgscv")
	assert.NoError(t, resp.Body.Close())

	// Make request to '/metrics' and assert response.
	resp, err = http.Get("http://127.0.0.1:5003/metrics")
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

	http.DefaultServeMux = new(http.ServeMux) // clean http environment (which may be dirtied by other concurrently running tests)
}

func Test_runSendMetricsLoop(t *testing.T) {
	// Run test read/write servers which will accept HTTP requests.
	readServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/metrics", r.URL.String())

		w.WriteHeader(http.StatusOK)

		_, err := w.Write([]byte(`"test_metric{example="example"}`))
		assert.NoError(t, err)
	}))
	defer readServer.Close()

	writeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.Greater(t, len(body), 0)
		assert.NoError(t, r.Body.Close())

		w.WriteHeader(http.StatusOK)
	}))
	defer writeServer.Close()

	// Prepare stuff, create repo with default 'system' service.
	config := &Config{
		ListenAddress:  strings.TrimPrefix(readServer.URL, "http://"),
		APIKey:         "TEST1234TEST-TEST-1234-TEST1234",
		SendMetricsURL: writeServer.URL, SendMetricsInterval: 600 * time.Millisecond,
	}
	repo := service.NewRepository()
	repo.AddServicesFromConfig(service.Config{
		ConnSettings: nil,
		ConnDefaults: nil,
	})

	// Run sending metrics to test server.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	assert.NoError(t, runSendMetricsLoop(ctx, config, repo))
}

func Test_addDelay(t *testing.T) {
	var dPrev time.Duration

	var loop int
	for i := 0; i < 20; i++ {
		loop++
		dCurr := addDelay(dPrev)
		assert.Greater(t, int64(dCurr-dPrev), int64(time.Duration(0)*time.Second))
		assert.Less(t, int64(dCurr-dPrev), int64(11*time.Second))
		dPrev = dCurr
		if dCurr >= 60*time.Second {
			break
		}
	}

	// at least 5 iterations should be done
	assert.Greater(t, loop, 5)
}
