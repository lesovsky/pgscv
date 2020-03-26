package app

import (
	"crypto/md5"
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/rs/zerolog"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"
)

// Start ...
// TODO: слишком длинная функция
func Start(c *Config) error {
	logger := c.Logger.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339})
	logger.Debug().Msg("start application")

	instanceRepo := NewServiceRepo(c)

	// if URLs specified use them instead of auto-discovery
	if c.URLStrings != nil {
		c.DiscoveryEnabled = false
		if err := instanceRepo.ConfigureServices(); err != nil {
			return err
		}
	} else {
		c.DiscoveryEnabled = true
		if err := instanceRepo.StartInitialDiscovery(); err != nil {
			return err
		}
		// TODO: что если там произойдет ошибка? по идее нужно делать ретрай
		go instanceRepo.StartBackgroundDiscovery()
	}

	logger.Debug().Msg("selecting mode")
	if c.MetricServiceBaseURL == "" {
		logger.Info().Msg("use PULL model, accepting requests on http://127.0.0.1:19090/metrics")

		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe("127.0.0.1:19090", nil); err != nil { // TODO: дефолтный порт должен быть другим
			return err
		}
	}

	logger.Info().Msgf("use PUSH model, sending metrics to %s every %d seconds", c.MetricServiceBaseURL, c.MetricsSendInterval/time.Second)

	// A job label is the special one which provides metrics uniqueness across several hosts and guarantees metrics will
	// not be overwritten on Pushgateway side. There is no other use-cases for this label, hence before ingesting by Prometheus
	// this label should be removed with 'metric_relabel_config' rule.
	jobLabelBase, err := getJobLabelBase(logger)
	if err != nil {
		return err
	}

	for {
		logger.Debug().Msgf("start job")
		var start = time.Now()

		// metrics for every discovered service is wrapped into a separate push
		for _, service := range instanceRepo.Services {
			jobLabel := fmt.Sprintf("db_system_%s_%s", jobLabelBase, service.ServiceID)
			var pusher = push.New(c.MetricServiceBaseURL, jobLabel)

			// if api-key specified use custom http-client and attach api-key to http requests
			if c.APIKey != "" {
				client := newHTTPClient(c.APIKey)
				pusher.Client(client)
			}

			// collect metrics for all discovered services
			pusher.Collector(service.Exporter)

			// push metrics
			if err := pusher.Add(); err != nil {
				// it is not critical error, just show it and continue
				logger.Warn().Err(err).Msg("could not push metrics")
			}
		}

		// sleep now
		logger.Debug().Msg("all jobs are finished, going to sleep")
		time.Sleep(time.Until(start.Add(c.MetricsSendInterval)))
	}

	// TODO: тупиковый for сверху, из него никак не выйти
	//return nil
}

// getJobLabelBase returns a unique string for job label. The string is based on machine-id or hostname.
func getJobLabelBase(logger zerolog.Logger) (string, error) {
	// try to use machine-id-based label
	machineID, err := getLabelByMachineID()
	if err == nil {
		return machineID, nil
	}

	// if getting machine-id failed, try to use hostname-based label
	logger.Warn().Err(err).Msgf("read machine-id failed, fallback to use hostname")
	machineID, err = getLabelByHostname()
	if err != nil {
		logger.Warn().Err(err).Msgf("get hostname failed, can't create job label")
		return "", err
	}
	return machineID, nil
}

// getLabelByMachineID reads /etc/machine-id and return its content
func getLabelByMachineID() (string, error) {
	if _, err := os.Stat("/etc/machine-id"); os.IsNotExist(err) {
		return "", err
	}
	content, err := ioutil.ReadFile("/etc/machine-id")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(content)), nil
}

// getLabelByHostname gets hostname and hashes it using MD5 and returns the hash
func getLabelByHostname() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", md5.Sum([]byte(hostname))), nil
}
