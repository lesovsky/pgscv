package app

import (
	"crypto/md5"
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/rs/zerolog"
	"net/http"
	"os"
	"time"
)

// Start ...
// TODO: слишком длинная функция
func Start(c *Config) error {
	logger := c.Logger.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339})
	logger.Debug().Msg("start application")

	instanceRepo := NewServiceRepo(c)
	if err := instanceRepo.StartInitialDiscovery(); err != nil {
		return err
	}

	// TODO: что если там произойдет ошибка? по идее нужно делать ретрай
	go instanceRepo.StartBackgroundDiscovery()

	logger.Debug().Msg("selecting mode")
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

	// A job label is the special one which provides metrics uniqueness across several hosts and guarantees metrics will
	// not be overwritten on Pushgateway side. There is no other use-cases for this label, hence before ingesting by Prometheus
	// this label should be removed with 'metric_relabel_config' rule.
	var jobLabel = "db_system_" + fmt.Sprintf("%x", md5.Sum([]byte(hostname)))

	for {
		logger.Debug().Msgf("start job")
		var start = time.Now()

		var pusher = push.New(c.MetricServiceBaseURL, jobLabel)

		// if api-key specified use custom http-client and attach api-key to http requests
		if c.APIKey != "" {
			client := newHTTPClient(c.APIKey)
			pusher.Client(client)
		}

		// collect metrics for all discovered services
		for _, service := range instanceRepo.Services {
			pusher.Collector(service.Exporter)
		}

		// push metrics
		if err := pusher.Add(); err != nil {
			// it is not critical error, just show it and continue
			logger.Warn().Err(err).Msg("could not push metrics")
		}

		// sleep now
		logger.Debug().Msg("job is finished, going to sleep")
		time.Sleep(time.Until(start.Add(c.MetricsSendInterval)))
	}

	// TODO: тупиковый for сверху, из него никак не выйти
	//return nil
}
