package pgscv

import (
	"context"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/barcodepro/pgscv/internal/service"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"
)

func Test_runPullMode(t *testing.T) {
	config := &Config{RuntimeMode: model.RuntimePullMode, ListenAddress: "127.0.0.1:5001"}
	wg := sync.WaitGroup{}

	// Running listener function with short-live context in concurrent goroutine.
	wg.Add(1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		err := runPullMode(ctx, config)
		assert.NoError(t, err)
		wg.Done()
	}()

	// Sleep little bit hoping it will be enough for running listener goroutine.
	time.Sleep(500 * time.Millisecond)

	// Make request to '/' and assert response.
	resp, err := http.Get("http://127.0.0.1:5001/")
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Contains(t, string(body), "pgSCV / Weaponry metric collector, for more info visit https://weaponry.io")
	assert.NoError(t, resp.Body.Close())

	// Make request to '/metrics' and assert response.
	resp, err = http.Get("http://127.0.0.1:5001/metrics")
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode, http.StatusOK)

	body, err = ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Contains(t, string(body), "go_gc_duration_seconds")
	assert.Contains(t, string(body), "process_cpu_seconds_total")
	assert.Contains(t, string(body), "promhttp_metric_handler_requests_in_flight")
	assert.NoError(t, resp.Body.Close())

	// Waiting for listener goroutine.
	wg.Wait()
}

func Test_runPushMode(t *testing.T) {
	// Run test server which will accept HTTP requests.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Regexp(t, regexp.MustCompile(`/metrics/job/db_system_[a-f0-9]{32}_system%3A0`), r.URL.String())
		body, err := ioutil.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.Greater(t, len(body), 0)

		w.WriteHeader(http.StatusOK)
	}))

	defer server.Close()

	// Prepare stuff, create repo with default 'system' service.
	config := &Config{APIKey: "TEST1234TEST-TEST-1234-TEST1234", ProjectID: 1, MetricsServiceURL: server.URL, MetricsSendInterval: 600 * time.Millisecond}
	repo := service.NewRepository()
	serviceConfig := service.Config{
		RuntimeMode:  model.RuntimePushMode,
		ProjectID:    strconv.Itoa(config.ProjectID),
		ConnSettings: nil,
		ConnDefaults: nil,
	}
	repo.AddServicesFromConfig(serviceConfig)
	assert.NoError(t, repo.SetupServices(serviceConfig))

	// Run pusher to send metrics to test server.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err := runPushMode(ctx, config, repo)
	assert.NoError(t, err)
}

func TestGetLabelByMachineID(t *testing.T) {
	s, err := getLabelByMachineID()
	assert.NoError(t, err)
	assert.Regexp(t, regexp.MustCompile(`[a-f0-9]{32}`), s)
}

func TestGetLabelByHostname(t *testing.T) {
	s, err := getLabelByHostname()
	assert.NoError(t, err)
	assert.Regexp(t, regexp.MustCompile(`[a-f0-9]{32}`), s)
}
