//
package main

import (
	"context"
	"fmt"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"os/signal"
	"pgscv/app"
	"pgscv/app/log"
	"pgscv/app/packaging"
	"syscall"
)

var (
	appName, gitCommit, gitBranch string
)

func main() {
	var (
		listenAddress        = kingpin.Flag("listen-address", "Address to listen on for metrics").Default("127.0.0.1:10090").Envar("LISTEN_ADDRESS").TCP()
		metricServiceBaseURL = kingpin.Flag("metric-service-url", "Metric service URL push to").Default("").Envar("METRIC_SERVICE_BASE_URL").URL()
		metricsSendInterval  = kingpin.Flag("send-interval", "Interval between pushes").Default("60s").Envar("SEND_INTERVAL").Duration()
		doBootstrap          = kingpin.Flag("bootstrap", "Run bootstrap, requires root privileges").Default("false").Envar("BOOTSTRAP").Bool()
		doUninstall          = kingpin.Flag("uninstall", "Run uninstall, requires root privileges").Default("false").Envar("UNINSTALL").Bool()
		apiKey               = kingpin.Flag("api-key", "Use api key").Default("").Envar("API_KEY").String()
		postgresPassword     = kingpin.Flag("pg-password", "Default password used for connecting to all discovered Postgres services").Default("").Envar("PG_PASSWORD").String()
		pgbouncerPassword    = kingpin.Flag("pgb-password", "Default password used for connecting to all discovered Pgbouncer services").Default("").Envar("PGB_PASSWORD").String()
		showver              = kingpin.Flag("version", "show version and exit").Default().Bool()
		logLevel             = kingpin.Flag("log-level", "set log level: debug, info, warn, error").Default("info").Envar("LOG_LEVEL").String()
	)
	kingpin.Parse()

	var sc = &app.Config{
		ListenAddress:        **listenAddress,
		MetricServiceBaseURL: **metricServiceBaseURL,
		MetricsSendInterval:  *metricsSendInterval,
		ProjectIDStr:         app.DecodeProjectIDStr(*apiKey),
		ScheduleEnabled:      false,
		APIKey:               *apiKey,
		BinaryName:           appName,
		BinaryVersion:        fmt.Sprintf("%s-%s", gitCommit, gitBranch),
		DefaultCredentials: app.DefaultCredentials{
			PostgresPassword:  *postgresPassword,
			PgbouncerPassword: *pgbouncerPassword,
		},
	}

	log.SetLevel(*logLevel)

	if *showver {
		fmt.Printf("%s %s\n", appName, sc.BinaryVersion)
		os.Exit(0)
	}

	if *doUninstall && *doBootstrap {
		log.Error("flags --uninstall and --bootstrap can not be used together, quit")
		os.Exit(1)
	}

	if *doUninstall {
		uc := &packaging.UninstallConfig{BinaryName: sc.BinaryName}
		os.Exit(packaging.RunUninstall(uc))
	}

	if *doBootstrap {
		bc := &packaging.BootstrapConfig{
			AgentBinaryName:          sc.BinaryName,
			MetricServiceBaseURL:     sc.MetricServiceBaseURL.String(),
			SendInterval:             sc.MetricsSendInterval,
			APIKey:                   sc.APIKey,
			DefaultPostgresPassword:  sc.PostgresPassword,
			DefaultPgbouncerPassword: sc.PgbouncerPassword,
		}
		os.Exit(packaging.RunBootstrap(bc))
	}

	if err := sc.Validate(); err != nil {
		log.Errorf("failed to start: %s", err)
		os.Exit(1)
	}

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	var doExit = make(chan error, 2)
	go func() {
		doExit <- listenSignals()
		cancel()
	}()

	go func() {
		doExit <- app.Start(ctx, sc)
	}()

	log.Infof("graceful shutdown: %s", <-doExit)
}

func listenSignals() error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return fmt.Errorf("got %s", <-c)
}
