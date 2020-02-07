package app

import (
	"crypto/md5"
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
	"net/http"
	"os"
	"time"
)

func Start(c *Config, repo *InstanceRepo) error {
	logger := c.Logger
	if c.MetricServiceBaseURL == "" {
		logger.Info().Msg("use PULL model, accepting requests on http://127.0.0.1:19090/metrics")

		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe("127.0.0.1:19090", nil); err != nil { // TODO: дефолтный порт должен быть другим
			return err
		}
	}

	logger.Info().Msgf("use PUSH model, sending metrics to %s every %d seconds", c.MetricServiceBaseURL, c.MetricsSendInterval/time.Second)
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	var garbageLabel = "db_system_" + fmt.Sprintf("%x", md5.Sum([]byte(hostname)))
	var pusher *push.Pusher

	for {
		// A garbage label is the special one which provides metrics uniqueness across several hosts and guarantees
		// metrics will not be overwritten on Pushgateway side. There is no other use-cases for this label, hence
		// before ingesting by Prometheus this label should be removed with 'metric_relabel_config' rule.
		pusher = push.New(c.MetricServiceBaseURL, garbageLabel)
		for _, instance := range repo.Instances {
			pusher.Collector(instance.Exporter)
		}

		if err := pusher.Add(); err != nil {
			// it is not critical error, just show it and continue
			logger.Warn().Msgf("%s: could not push metrics: %s", time.Now().Format("2006-01-02T15:04:05.999"), err)
		}
		time.Sleep(c.MetricsSendInterval)
	}

	// TODO: тупиковый for сверху, из него никак не выйти
	return nil
}
