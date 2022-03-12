package pgscv

import (
	"context"
	"github.com/lesovsky/pgscv/internal/http"
	"github.com/lesovsky/pgscv/internal/service"
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestStart(t *testing.T) {
	writeSrv := http.TestServer(t, http.StatusOK, "")
	defer writeSrv.Close()

	// Create app config.
	config := &Config{
		ListenAddress:  "127.0.0.1:5002",
		APIKey:         "TEST1234TEST-TEST-1234-TEST1234",
		SendMetricsURL: writeSrv.URL, SendMetricsInterval: 1 * time.Second,
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

func Test_runSendMetricsLoop(t *testing.T) {
	readSrv := http.TestServer(t, http.StatusOK, "example 1")
	defer readSrv.Close()

	writeSrv := http.TestServer(t, http.StatusOK, "")
	defer writeSrv.Close()

	// Prepare stuff, create repo with default 'system' service.
	config := &Config{
		ListenAddress:  strings.TrimPrefix(readSrv.URL, "http://"),
		APIKey:         "TEST1234TEST-TEST-1234-TEST1234",
		SendMetricsURL: writeSrv.URL, SendMetricsInterval: 600 * time.Millisecond,
	}
	repo := service.NewRepository()
	repo.AddServicesFromConfig(service.Config{
		ConnsSettings: nil,
		ConnDefaults:  nil,
	})

	// Run sending metrics to test server.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	assert.NoError(t, runSendMetricsLoop(ctx, config, repo))
}

func Test_readLastSendTS(t *testing.T) {
	testcases := []struct {
		from string
		want int64
	}{
		{from: "testdata/pgscv-last-send-ts.golden", want: 1618981876},
		{from: "testdata/pgscv-last-send-ts.invalid", want: 0},
		{from: "testdata/pgscv-last-send-ts.unknown", want: 0},
	}

	for _, tc := range testcases {
		assert.Equal(t, tc.want, readLastSendTS(tc.from))
	}
}

func Test_writeLastSendTS(t *testing.T) {
	now := time.Now().Unix()
	f, err := os.CreateTemp("/tmp", "pgscv-testing-writeLastSendTS.tmp")
	assert.NoError(t, err)
	testfile := f.Name()
	assert.NoError(t, f.Close())

	writeLastSendTS(now, testfile)

	value := readLastSendTS(testfile)
	assert.Equal(t, now, value)

	assert.NoError(t, os.Remove(testfile))
}

func Test_lastSendStaleness(t *testing.T) {
	in := time.Now().Add(-20 * time.Second).Unix()

	got := lastSendStaleness(in, time.Minute)
	assert.Greater(t, int64(got), int64(time.Second))
	assert.Less(t, int64(got), int64(41*time.Second))

	// test stale
	in = time.Now().Add(-120 * time.Second).Unix()

	got = lastSendStaleness(in, time.Minute)
	assert.Equal(t, int64(got), int64(0))

	// test zero -- should be also stale
	got = lastSendStaleness(0, time.Minute)
	assert.Equal(t, int64(got), int64(0))
}

func Test_sendClient_readMetrics(t *testing.T) {
	readSrv := http.TestServer(t, http.StatusOK, "example 1")
	defer readSrv.Close()

	sc, err := newSendClient(&Config{
		ListenAddress: strings.TrimPrefix(readSrv.URL, "http://"),
		APIKey:        "example",
	})
	assert.NoError(t, err)

	got, err := sc.readMetrics()
	assert.NoError(t, err)
	assert.Equal(t, "example 1", string(got))
}

func Test_sendClient_sendMetrics(t *testing.T) {
	readSrv := http.TestServer(t, http.StatusOK, "example 1")
	defer readSrv.Close()

	writeSrv := http.TestServer(t, http.StatusOK, "")
	defer writeSrv.Close()

	sc, err := newSendClient(&Config{
		ListenAddress:  strings.TrimPrefix(readSrv.URL, "http://"),
		SendMetricsURL: writeSrv.URL,
		APIKey:         "example",
	})
	assert.NoError(t, err)

	assert.NoError(t, sc.sendMetrics([]byte("example 1")))
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
