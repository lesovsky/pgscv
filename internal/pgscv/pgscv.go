package pgscv

import (
	"context"
	"fmt"
	"github.com/lesovsky/pgscv/internal/http"
	"github.com/lesovsky/pgscv/internal/log"
	"github.com/lesovsky/pgscv/internal/packaging/autoupdate"
	"github.com/lesovsky/pgscv/internal/service"
	"io"
	"math"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"
)

// Start is the application's starting point.
func Start(ctx context.Context, config *Config) error {
	log.Debug("start application")

	serviceRepo := service.NewRepository()

	serviceConfig := service.Config{
		NoTrackMode:        config.NoTrackMode,
		ConnDefaults:       config.Defaults,
		ConnsSettings:      config.ServicesConnsSettings,
		DatabasesRE:        config.DatabasesRE,
		DisabledCollectors: config.DisableCollectors,
		CollectorsSettings: config.CollectorsSettings,
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(ctx)

	if config.ServicesConnsSettings == nil || len(config.ServicesConnsSettings) == 0 {
		// run background discovery, the service repo will be fulfilled at first iteration
		wg.Add(1)
		go func() {
			serviceRepo.StartBackgroundDiscovery(ctx, serviceConfig)
			wg.Done()
		}()
	} else {
		// fulfill service repo using passed services
		serviceRepo.AddServicesFromConfig(serviceConfig)

		// setup exporters for all services
		err := serviceRepo.SetupServices(serviceConfig)
		if err != nil {
			cancel()
			return err
		}
	}

	// Start auto-update loop if it is enabled.
	if config.AutoUpdate != "off" {
		wg.Add(1)
		go func() {
			ac := &autoupdate.Config{
				BinaryPath:    config.BinaryPath,
				BinaryVersion: config.BinaryVersion,
				UpdatePolicy:  config.AutoUpdate,
			}
			autoupdate.StartAutoupdateLoop(ctx, ac)
			wg.Done()
		}()
	}

	errCh := make(chan error)
	defer close(errCh)

	// Start HTTP metrics listener.
	wg.Add(1)
	go func() {
		if err := runMetricsListener(ctx, config); err != nil {
			errCh <- err
		}
		wg.Done()
	}()

	// Start metrics sender if necessary.
	if config.SendMetricsURL != "" {
		wg.Add(1)
		go func() {
			if err := runSendMetricsLoop(ctx, config, serviceRepo); err != nil {
				errCh <- err
			}
			wg.Done()
		}()
	}

	// Waiting for errors or context cancelling.
	for {
		select {
		case <-ctx.Done():
			log.Info("exit signaled, stop application")
			cancel()
			wg.Wait()
			return nil
		case err := <-errCh:
			cancel()
			wg.Wait()
			return err
		}
	}
}

// runMetricsListener start HTTP listener accordingly to passed configuration.
func runMetricsListener(ctx context.Context, config *Config) error {
	srv := http.NewServer(http.ServerConfig{
		Addr:       config.ListenAddress,
		AuthConfig: config.AuthConfig,
	})

	errCh := make(chan error)
	defer close(errCh)

	// Run default listener.
	go func() {
		errCh <- srv.Serve()
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

// runSendMetricsLoop starts infinite loop with periodic metric sending until it's interrupted.
func runSendMetricsLoop(ctx context.Context, config *Config, instanceRepo *service.Repository) error {
	const lastSendTSFile = "/tmp/pgscv-last-send.timestamp"

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

	// Initialize last send timestamp from file.
	lastSendTS := readLastSendTS(lastSendTSFile)

	// Do one-time sleep depending on last send timestamp staleness.
	time.Sleep(lastSendStaleness(lastSendTS, config.SendMetricsInterval))

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

		lastSendTS = time.Now().Unix()

		err = sendClient.sendMetrics(buf)
		if err != nil {
			delay = addDelay(delay)
			log.Infof("send metrics failed: %s, retry after %s", err, delay.String())
			continue
		}

		// Reading and sending successful, reset delay.
		delay = 0

		// Update last successful send timestamp, in case of pgSCV restarts
		writeLastSendTS(lastSendTS, lastSendTSFile)

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

// Last send timestamp.
// Data model of timeseries databases (Prometheus or VictoriaMetrics) relies on
// interval between stored datapoints. The best case that interval is consistent
// across long period of time. This easy to follow when agent is scraped by
// external system (like Prometheus, Vmagent, etc). In case of sending metrics,
// agent should follow the configured sending interval across agent restarts.
// For keeping the sending interval, pgSCV saves UNIX timestamp of the last
// sending attempt into the file. After restart, pgSCV at first reading the file,
// and if it is found and has valid timestamp, agent correct when to make next
// metric sending.

// readLastSendTS read last send timestamp from file and return its value.
func readLastSendTS(from string) int64 {
	content, err := os.ReadFile(from) // #nosec G304
	if err != nil {
		log.Warnf("%s; last send timestamp will be reinitialized", err)
		return 0
	}

	v, err := strconv.ParseInt(string(content), 10, 64)
	if err != nil {
		log.Warnf("invalid input, parse %s failed; last send timestamp will be reinitialized", err)
		return 0
	}

	return v
}

// writeLastSendTS writes passed last timestamp value and write it to file.
func writeLastSendTS(v int64, to string) {
	data := []byte(fmt.Sprintf("%d", v))
	err := os.WriteFile(to, data, 0600)
	if err != nil {
		log.Warnf("write last send timestamp failed: %s; skip", err)
	}
}

// lastSendStaleness calculate how much time before last send timestamp become stale.
func lastSendStaleness(v int64, limit time.Duration) time.Duration {
	delta := time.Now().Unix() - v

	// timestamp since last send exceeds limit, means last send is already stale.
	if (time.Duration(delta) * time.Second) > limit {
		return 0
	}

	// timestamp since last send does not exceed limit, return how many seconds left before stale.
	return limit - (time.Duration(delta) * time.Second)
}

// sendClient defines worker which read metrics from local source and send metrics to remote URL.
type sendClient struct {
	apiKey      string   // API key used for communicating with remote HTTP service
	hostname    string   // System hostname used as value of 'instance'
	readURL     *url.URL // local URL for reading metrics
	readClient  *http.Client
	writeURL    *url.URL // remote URL for sending metrics
	writeClient *http.Client
}

// newSendClient creates new sendClient.
func newSendClient(config *Config) (sendClient, error) {
	readURL, err := url.Parse("http://" + config.ListenAddress + "/metrics")
	if err != nil {
		return sendClient{}, err
	}

	writeURL, err := url.Parse(config.SendMetricsURL)
	if err != nil {
		return sendClient{}, err
	}

	hostname, err := os.Hostname()
	if err != nil {
		return sendClient{}, err
	}

	return sendClient{
		apiKey:      config.APIKey,
		hostname:    hostname,
		readURL:     readURL,
		readClient:  http.NewClient(http.ClientConfig{Timeout: 10 * time.Second}),
		writeURL:    writeURL,
		writeClient: http.NewClient(http.ClientConfig{Timeout: 10 * time.Second}),
	}, nil
}

// readMetrics read metrics from configured URL and returns response.
func (s *sendClient) readMetrics() ([]byte, error) {
	resp, err := s.readClient.Get(s.readURL.String())
	if err != nil {
		return nil, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Read and close the rest of body.
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	return body, nil
}

// sendMetrics wrap buffer data into POST HTTP request and send to remote URL.
func (s *sendClient) sendMetrics(buf []byte) error {
	req, err := http.NewPushRequest(s.writeURL.String(), s.apiKey, s.hostname, buf)
	if err != nil {
		return err
	}

	err = http.DoPushRequest(s.writeClient, req)
	if err != nil {
		return err
	}

	return nil
}

// addDelay increments passed delay to random value between 1 and 10 seconds.
func addDelay(d time.Duration) time.Duration {
	rand.Seed(time.Now().UnixNano())

	sec := int(math.Max(float64(d/time.Second), 1))
	sec = int(math.Min(float64(sec+rand.Intn(9))+1, 60)) // #nosec G404

	return time.Duration(sec) * time.Second
}
