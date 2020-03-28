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

// Start is the application's main entry point
func Start(c *Config) error {
	logger := c.Logger.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339})
	logger.Debug().Msg("start application")

	serviceRepo := NewServiceRepo(c)

	if err := serviceRepo.discoverServicesOnce(); err != nil {
		return err
	}

	go func() {
		// TODO: что если там произойдет ошибка? по идее нужно делать ретрай
		serviceRepo.startBackgroundDiscovery()
	}()

	switch c.RuntimeMode {
	case runtimeModePull:
		return runPullMode(c)
	case runtimeModePush:
		return runPushMode(c, serviceRepo)
	default:
		logger.Error().Msgf("unknown mode selected: %d, quit", c.RuntimeMode)
		return nil
	}
}

// runPullMode runs application in PULL mode (accepts requests for metrics via HTTP)
func runPullMode(config *Config) error {
	config.Logger.Info().Msgf("use PULL mode, accepting requests on http://%s/metrics", config.ListenAddress.String())

	http.Handle("/metrics", promhttp.Handler())
	return http.ListenAndServe(config.ListenAddress.String(), nil)
}

// runPushMode runs application in PUSH mode - with interval collects metrics and push them to remote service
func runPushMode(config *Config, instanceRepo *ServiceRepo) error {
	config.Logger.Info().Msgf("use PUSH mode, sending metrics to %s every %d seconds", config.MetricServiceBaseURL.String(), config.MetricsSendInterval/time.Second)

	// A job label is the special one which provides metrics uniqueness across several hosts and guarantees metrics will
	// not be overwritten on Pushgateway side. There is no other use-cases for this label, hence before ingesting by Prometheus
	// this label should be removed with 'metric_relabel_config' rule.
	jobLabelBase, err := getJobLabelBase(config.Logger)
	if err != nil {
		return err
	}

	for {
		config.Logger.Debug().Msgf("start job")
		var start = time.Now()

		// metrics for every discovered service is wrapped into a separate push
		for _, service := range instanceRepo.Services {
			jobLabel := fmt.Sprintf("db_system_%s_%s", jobLabelBase, service.ServiceID)
			var pusher = push.New(config.MetricServiceBaseURL.String(), jobLabel)

			// if api-key specified use custom http-client and attach api-key to http requests
			if config.APIKey != "" {
				client := newHTTPClient(config.APIKey)
				pusher.Client(client)
			}

			// collect metrics for all discovered services
			pusher.Collector(service.Exporter)

			// push metrics
			if err := pusher.Add(); err != nil {
				// it is not critical error, just show it and continue
				config.Logger.Warn().Err(err).Msg("could not push metrics")
			}
		}

		// sleep now
		config.Logger.Debug().Msg("all jobs are finished, going to sleep")
		time.Sleep(time.Until(start.Add(config.MetricsSendInterval)))
	}
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
