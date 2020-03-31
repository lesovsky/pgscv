//
package main

import (
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"os/signal"
	"pgscv/app"
	"syscall"
	"time"
)

var (
	appName, gitCommit, gitBranch string
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339})
	//log.Logger = log.With().Caller().Logger().Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339})

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
		Logger:               log.Logger,
		ListenAddress:        **listenAddress,
		MetricServiceBaseURL: **metricServiceBaseURL,
		MetricsSendInterval:  *metricsSendInterval,
		ProjectIDStr:         app.DecodeProjectIDStr(*apiKey),
		ScheduleEnabled:      false,
		APIKey:               *apiKey,
		BinaryName:           appName,
		DefaultCredentials: app.DefaultCredentials{
			PostgresPassword:  *postgresPassword,
			PgbouncerPassword: *pgbouncerPassword,
		},
	}

	switch *logLevel {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	if *showver {
		fmt.Printf("%s %s-%s\n", appName, gitCommit, gitBranch)
		os.Exit(0)
	}

	if *doUninstall && *doBootstrap {
		log.Logger.Error().Msg("flags --uninstall and --bootstrap can not be used together, quit")
		os.Exit(1)
	}

	if *doUninstall {
		os.Exit(app.RunUninstall(sc))
	}

	if *doBootstrap {
		os.Exit(app.RunBootstrap(sc))
	}

	if err := sc.Validate(); err != nil {
		log.Logger.Err(err).Msgf("failed to start")
	}

	var doExit = make(chan error, 2)
	go func() {
		doExit <- listenSignals()
	}()

	go func() {
		doExit <- app.Start(sc)
	}()

	log.Info().Msgf("graceful shutdown: %s", <-doExit)
}

func listenSignals() error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT|syscall.SIGTERM)
	return fmt.Errorf("got %s", <-c)
}
