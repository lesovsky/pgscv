package pgscv

import (
	"context"
	"errors"
	"github.com/lesovsky/pgscv/internal/http"
	"github.com/lesovsky/pgscv/internal/log"
	"github.com/lesovsky/pgscv/internal/service"
	"sync"
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

	if len(config.ServicesConnsSettings) == 0 {
		return errors.New("no services defined")
	}

	// fulfill service repo using passed services
	serviceRepo.AddServicesFromConfig(serviceConfig)

	// setup exporters for all services
	err := serviceRepo.SetupServices(serviceConfig)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup

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

	// Waiting for errors or context cancelling.
	for {
		select {
		case <-ctx.Done():
			log.Info("exit signaled, stop application")
			cancel()
			wg.Wait()
			return nil
		case e := <-errCh:
			cancel()
			wg.Wait()
			return e
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
