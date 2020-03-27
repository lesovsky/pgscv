package app

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

const (
	// how many metrics in percent might be absent during tests without failing
	absentMetricsThreshold float64 = 10
)

func TestGetLabelByHostname(t *testing.T) {
	s, err := getLabelByHostname()
	assert.NoError(t, err)
	assert.NotEmpty(t, s)
}

func TestGetLabelByMachineID(t *testing.T) {
	s, err := getLabelByMachineID()
	assert.NoError(t, err)
	assert.NotEmpty(t, s)
}

// Test_runPullMode тест проверяет работу PULL режима, делает запрос к prom-хэндлеру и смотрит сколько метрик тот возвращает.
// В текущей реализации нельзя вернуть абсолютно все метрики, поэтому ориентируемся на пороговое значение absentMetricsThreshold
// Если метрик отсутствует больше чем absentMetricsThreshold, то это повод волноваться.
func Test_runPullMode(t *testing.T) {
	if uid := os.Geteuid(); uid != 0 {
		t.Skipf("root privileges required, skip")
	}

	var c = &Config{
		Logger:           zerolog.Logger{},
		DiscoveryEnabled: true,
		//URLStrings: []string{"postgres://postgres@127.0.0.1/postgres"},
		Credentials: Credentials{
			PostgresUser:  "weaponry_app",
			PostgresPass:  "lessqqmorepewpew",
			PgbouncerUser: "weaponry_app",
			PgbouncerPass: "lessqqmorepewpew",
		},
	}

	repo := NewServiceRepo(c)
	assert.NotNil(t, repo)
	assert.NoError(t, repo.Configure(c))

	ts := httptest.NewServer(promhttp.Handler())
	defer ts.Close()

	res, err := http.Get(ts.URL)
	assert.NoError(t, err)
	assert.NotNil(t, res)

	content, err := ioutil.ReadAll(res.Body)
	assert.NoError(t, err)
	assert.NoError(t, res.Body.Close())
	assert.NotNil(t, content)

	var total, absent float64
	for k, v := range globalHelpCatalog() {
		total++
		if !strings.Contains(string(content), fmt.Sprintf("# HELP %s %s", k, v)) {
			t.Logf("absent %s", k)
			absent++
		}
	}
	pct := 100 * absent / total
	t.Logf("metrics: total %.0f, absent %.0f, absent %.2f%%\n", total, absent, pct)
	assert.Less(t, pct, absentMetricsThreshold)

	for i := range repo.Services {
		repo.removeService(repo.Services[i].Pid)
	}
}
