package app

import (
	"context"
	"crypto/md5"
	"fmt"
	"github.com/barcodepro/pgscv/service/internal/log"
	"github.com/barcodepro/pgscv/service/internal/packaging"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"
)

func Start(ctx context.Context, config *Config) error {
	log.Debug("start application")

	serviceRepo := NewServiceRepo()

	if config.ServicesConnSettings == nil {
		// run background discovery, the service repo will be fulfilled at first iteration
		go func() {
			serviceRepo.startBackgroundDiscovery(ctx, config)
		}()
	} else {
		// fulfill service repo using passed services
		serviceRepo.addServicesFromConfig(config)

		// setup exporters for all services
		err := serviceRepo.setupServices(config)
		if err != nil {
			return err
		}
	}

	go func() {
		ac := &packaging.AutoupdateConfig{
			BinaryVersion: config.BinaryVersion,
		}
		packaging.StartBackgroundAutoUpdate(ctx, ac)
	}()

	switch config.RuntimeMode {
	case runtimeModePull:
		return runPullMode(config)
	case runtimeModePush:
		return runPushMode(ctx, config, serviceRepo)
	default:
		log.Errorf("unknown mode selected: %d, quit", config.RuntimeMode)
		return nil
	}
}

// runPullMode runs application in PULL mode (accepts requests for metrics via HTTP)
func runPullMode(config *Config) error {
	log.Infof("use PULL mode, accepting requests on http://%s/metrics", config.ListenAddress)

	http.Handle("/metrics", promhttp.Handler())
	return http.ListenAndServe(config.ListenAddress, nil)
}

// runPushMode runs application in PUSH mode - with interval collects metrics and push them to remote service
func runPushMode(ctx context.Context, config *Config, instanceRepo *ServiceRepo) error {
	// A job label is the special one which provides metrics uniqueness across several hosts and guarantees metrics will
	// not be overwritten on Pushgateway side. There is no other use-cases for this label, hence before ingesting by Prometheus
	// this label should be removed with 'metric_relabel_config' rule.
	jobLabelBase, err := getJobLabelBase()
	if err != nil {
		return err
	}

	// Before sending metrics wait until any services appear in the repo, else need to wait an one MetricsSendInterval.
	// This is the one-time operation and here is using a naive approach with 'for loop + sleep' instead of channels/sync stuff.
	for {
		if instanceRepo.totalServices() > 0 {
			break
		}
		time.Sleep(time.Second)
	}

	log.Infof("use PUSH mode, sending metrics to %s every %d seconds", config.MetricsServiceURL, config.MetricsSendInterval/time.Second)

	ticker := time.NewTicker(config.MetricsSendInterval)
	for {
		// push metrics to the remote service
		pushMetrics(jobLabelBase, config.MetricsServiceURL, config.APIKey, instanceRepo)

		// sleeping for next iteration
		select {
		case <-ticker.C:
			continue
		case <-ctx.Done():
			log.Info("exit signaled, stop pushing metrics")
			ticker.Stop()
			return nil
		}
	}
}

// pushMetrics collects metrics for discovered services and pushes them to remote service
func pushMetrics(labelBase string, url string, apiKey string, repo *ServiceRepo) {
	log.Debug("job started")

	var servicesIDs = repo.getServiceIDs()

	// metrics for every discovered service is wrapped into a separate push
	for _, id := range servicesIDs {
		var service = repo.getService(id)
		if service.Exporter == nil {
			log.Infof("service [%s] exists, but exporter is not initialized yet, skip", service.ServiceID)
			continue
		}

		jobLabel := fmt.Sprintf("db_system_%s_%s", labelBase, service.ServiceID)
		var pusher = push.New(url, jobLabel)

		// if api-key specified use custom http-client and attach api-key to http requests
		if apiKey != "" {
			client := newHTTPClient(apiKey)
			pusher.Client(client)
		}

		// collect metrics for all discovered services
		pusher.Collector(service.Exporter)

		// push metrics
		if err := pusher.Add(); err != nil {
			// it is not critical error, just show it and continue
			log.Warnln("could not push metrics: ", err)
		}
	}

	log.Debug("job finished")
}

// getJobLabelBase returns a unique string for job label. The string is based on machine-id or hostname.
func getJobLabelBase() (string, error) {
	log.Debug("calculating job label for pushed metrics")

	// try to use machine-id-based label
	machineID, err := getLabelByMachineID()
	if err == nil {
		return machineID, nil
	}

	// if getting machine-id failed, try to use hostname-based label
	log.Warnln("read machine-id failed, fallback to use hostname: ", err)
	machineID, err = getLabelByHostname()
	if err != nil {
		log.Warnln("get hostname failed, can't create job label: ", err)
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
