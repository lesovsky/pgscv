package pgscv

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/packaging/autoupdate"
	"github.com/weaponry/pgscv/internal/service"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"time"
)

func Start(ctx context.Context, config *Config) error {
	log.Debug("start application")

	serviceRepo := service.NewRepository()

	serviceConfig := service.Config{
		NoTrackMode:        config.NoTrackMode,
		ConnDefaults:       config.Defaults,
		ConnSettings:       config.ServicesConnSettings,
		Filters:            config.Filters,
		DisabledCollectors: config.DisableCollectors,
	}

	if config.ServicesConnSettings == nil {
		// run background discovery, the service repo will be fulfilled at first iteration
		go func() {
			serviceRepo.StartBackgroundDiscovery(ctx, serviceConfig)
		}()
	} else {
		// fulfill service repo using passed services
		serviceRepo.AddServicesFromConfig(serviceConfig)

		// setup exporters for all services
		err := serviceRepo.SetupServices(serviceConfig)
		if err != nil {
			return err
		}
	}

	// Start auto-update loop if source URL is specified.
	if config.AutoUpdateURL != "" {
		go func() {
			ac := &autoupdate.Config{
				BinaryPath:    config.BinaryPath,
				BinaryVersion: config.BinaryVersion,
			}
			autoupdate.StartAutoupdateLoop(ctx, ac)
		}()
	}

	var errCh = make(chan error)
	defer close(errCh)

	// Start HTTP metrics listener.
	go func() {
		if err := runMetricsListener(ctx, config); err != nil {
			errCh <- err
		}
	}()

	// Start metrics sender if necessary.
	if config.SendMetricsURL != "" {
		go func() {
			if err := runSendMetricsLoop(ctx, config, serviceRepo); err != nil {
				errCh <- err
			}
		}()
	}

	// Waiting for errors or context cancelling.
	for {
		select {
		case <-ctx.Done():
			log.Info("exit signaled, stop application")
			return nil
		case err := <-errCh:
			return err
		}
	}
}

func runMetricsListener(ctx context.Context, config *Config) error {
	log.Infof("accepting requests on http://%s/metrics", config.ListenAddress)

	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte(`<html>
			<head><title>pgSCV / Weaponry metric collector</title></head>
			<body>
			<h1>pgSCV / Weaponry metric collector, for more info visit https://weaponry.io</h1>
			<p><a href="/metrics">Metrics</a></p>
			</body>
			</html>`))
		if err != nil {
			log.Warnln("response write failed: ", err)
		}
	})

	var errCh = make(chan error)
	defer close(errCh)

	// Run listener.
	go func() {
		errCh <- http.ListenAndServe(config.ListenAddress, nil)
	}()

	// Waiting for errors or context cancelling.
	for {
		select {
		case <-ctx.Done():
			log.Info("exit signaled, stop metrics listener")
			return nil
		case err := <-errCh:
			return err
		}
	}
}

func runSendMetricsLoop(ctx context.Context, config *Config, instanceRepo *service.Repository) error {
	log.Infof("sending metrics to %s every %d seconds", config.SendMetricsURL, config.SendMetricsInterval/time.Second)

	// Before sending metrics wait until any services appear in the repo, else need to wait an one MetricsSendInterval.
	// This is the one-time operation and here is using a naive approach with 'for loop + sleep' instead of channels/sync stuff.
	log.Debugln("waiting for services appear in service repo...")
	for {
		time.Sleep(time.Second)
		if n := instanceRepo.TotalServices(); n > 0 {
			log.Debugln("done, services found: ", n)
			break
		}
	}

	sendClient, err := newSendClient(config)
	if err != nil {
		return err
	}

	ticker := time.NewTicker(config.SendMetricsInterval)
	var delay time.Duration
	for {
		if delay > 0 {
			log.Debugf("waiting for delay %s", delay.String())
			time.Sleep(delay)
		}

		buf, err := sendClient.readMetrics()
		if err != nil {
			delay = time.Second
			log.Infof("read metrics failed: %s, retry after %s", err, delay.String())
			continue
		}

		err = sendClient.sendMetrics(buf)
		if err != nil {
			delay = addDelay(delay)
			log.Infof("send metrics failed: %s, retry after %s", err, delay.String())
			continue
		}

		// Reading and sending successful, reset delay.
		delay = 0

		// Sleeping for next iteration.
		select {
		case <-ctx.Done():
			log.Info("exit signaled, stop metrics sending")
			ticker.Stop()
			return nil
		case <-ticker.C:
			continue
		}
	}
}

// sendClient ...
type sendClient struct {
	apiKey   string
	readURL  *url.URL
	writeURL *url.URL
	timeout  time.Duration
	Client   *http.Client
}

// newSendClient ...
func newSendClient(config *Config) (sendClient, error) {
	readURL, err := url.Parse("http://" + config.ListenAddress + "/metrics")
	if err != nil {
		return sendClient{}, err
	}

	writeURL, err := url.Parse(config.SendMetricsURL)
	if err != nil {
		return sendClient{}, err
	}

	return sendClient{
		apiKey:   config.APIKey,
		readURL:  readURL,
		writeURL: writeURL,
		timeout:  10 * time.Second,
		Client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:    5,
				IdleConnTimeout: 120 * time.Second,
			},
			Timeout: 10 * time.Second,
		},
	}, nil
}

// readMetrics ...
func (s *sendClient) readMetrics() ([]byte, error) {
	resp, err := http.Get(s.readURL.String())
	if err != nil {
		return nil, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	return body, nil
}

// sendMetrics ...
func (s *sendClient) sendMetrics(buf []byte) error {
	log.Debugln("start sending metrics")

	req, err := http.NewRequest("POST", s.writeURL.String(), bytes.NewReader(buf))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/text")
	req.Header.Set("User-Agent", "pgSCV")
	req.Header.Add("X-Weaponry-Api-Key", s.apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	req = req.WithContext(ctx)

	resp, err := s.Client.Do(req)
	if err != nil {
		return err
	}

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(resp.Body, 512))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		return fmt.Errorf("server returned HTTP status %s: %s", resp.Status, line)
	}

	log.Debugln("sending metrics finished successfully: server returned HTTP status ", resp.Status)
	return nil
}

// addDelay increments passed delay to random value between 1 and 10 seconds.
func addDelay(d time.Duration) time.Duration {
	sec := int(math.Max(float64(d/time.Second), 1))
	sec = int(math.Min(float64(sec+rand.New(rand.NewSource(time.Now().Unix())).Intn(9))+1, 60)) // #nosec G404

	return time.Duration(sec) * time.Second
}
