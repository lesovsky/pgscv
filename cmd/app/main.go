//
package main

import (
	"fmt"
	"github.com/rs/zerolog"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"scout/app"
	"time"
)

var (
	appName, gitCommit, gitBranch string
)

func main() {
	var (
		logger               = zerolog.New(os.Stdout).With().Timestamp().Str("service", appName).Logger().Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339})
		metricServiceBaseURL = kingpin.Flag("metric-service-url", "Metric service URL push to").Default("").Envar("METRIC_SERVICE_BASE_URL").String()
		metricsSendInterval  = kingpin.Flag("send-interval", "Interval between pushes").Default("60s").Envar("SEND_INTERVAL").Duration()
		projectIdStr         = kingpin.Flag("projectid", "Project identifier string").Envar("PROJECTID").String()
		bootstrapKey         = kingpin.Flag("bootstrap-key", "Run bootstrap using specified key, requires root privileges").Envar("BOOTSTRAP_KEY").String()
		showver              = kingpin.Flag("version", "show version and exit").Default().Bool()
		logLevel             = kingpin.Flag("log-level", "set log level: debug, info, warn, error").Default("info").Envar("LOG_LEVEL").String()
	)
	kingpin.Parse()

	var sc = &app.Config{
		Logger:               logger,
		MetricServiceBaseURL: *metricServiceBaseURL,
		MetricsSendInterval:  *metricsSendInterval,
		ProjectIdStr:         *projectIdStr,
		ScheduleEnabled:      false,
		BootstrapKey:         *bootstrapKey,
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

	if sc.BootstrapKey != "" {
		os.Exit(app.RunBootstrap(sc.BootstrapKey))
	}

	// обязательно должен быть
	if sc.ProjectIdStr == "" {
		logger.Fatal().Msg("project identifier is not specified")
	}

	// use schedulers in push mode
	if sc.MetricServiceBaseURL != "" {
		sc.ScheduleEnabled = true
	}

	if err := app.Start(sc); err != nil {
		logger.Error().Err(err)
	}

	logger.Info().Msg("Graceful shutdown")
}
