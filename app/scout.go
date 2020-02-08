package app

import (
	"crypto/md5"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
	"net/http"
	"os"
	"scout/app/discovery"
	"time"
)

// TODO: слишком длинная функция
func Start(c *Config) error {
	logger := c.Logger

	dc := &discovery.DiscoveryConfig{
		Logger: logger,
		//ServiceMode: serviceMode,
		ProjectIdStr:    c.ProjectIdStr,
		ScheduleEnabled: c.ScheduleEnabled,
	}

	instanceRepo := discovery.NewServiceRepo(dc)
	if err := instanceRepo.StartInitialDiscovery(); err != nil {
		return err
	}

	// TODO: что если там произойдет ошибка? по идее нужно делать ретрай
	go instanceRepo.StartBackgroundDiscovery()

	// создаем экспортер для экземпляра инстанса, затем помещаем созданный экспортер в экземпляр
	var err error
	for i, service := range instanceRepo.Services {
		exporter, err := NewExporter(service, instanceRepo)
		if err != nil {
			return err
		}

		// для PULL режима надо зарегать новоявленного экспортера, для PUSH это сделается в процессе самого пуша
		if c.MetricServiceBaseURL == "" {
			prometheus.MustRegister(exporter)
			logger.Info().Msgf("auto-discovery: exporter registered for %s with pid %d", service.ServiceId, service.Pid)
		}
		service.Exporter = exporter
		instanceRepo.Services[i] = service
	}

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
		for _, service := range instanceRepo.Services {
			pusher.Collector(service.Exporter)
		}

		if err := pusher.Add(); err != nil {
			// it is not critical error, just show it and continue
			logger.Warn().Err(err).Msg("could not push metrics")
		}
		time.Sleep(c.MetricsSendInterval)
	}

	// TODO: тупиковый for сверху, из него никак не выйти
	return nil
}
