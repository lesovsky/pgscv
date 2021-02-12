package main

import (
	"context"
	"fmt"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/packaging"
	"github.com/weaponry/pgscv/internal/pgscv"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"os/signal"
	"syscall"
)

var (
	appName, gitTag, gitCommit, gitBranch string
)

func main() {
	var (
		showVersion = kingpin.Flag("version", "show version and exit").Default().Bool()
		logLevel    = kingpin.Flag("log-level", "set log level: debug, info, warn, error").Default("info").Envar("LOG_LEVEL").String()
		configFile  = kingpin.Flag("config-file", "path to config file").Default("/etc/pgscv.yaml").Envar("PGSCV_CONFIG_FILE").String()
		doBootstrap = kingpin.Flag("bootstrap", "run bootstrap, requires root privileges").Default("false").Envar("PGSCV_BOOTSTRAP").Bool()
		doUninstall = kingpin.Flag("uninstall", "run uninstall, requires root privileges").Default("false").Envar("PGSCV_UNINSTALL").Bool()
	)
	kingpin.Parse()
	log.SetLevel(*logLevel)
	log.SetApplication(appName)

	if *showVersion {
		fmt.Printf("%s %s %s-%s\n", appName, gitTag, gitCommit, gitBranch)
		os.Exit(0)
	}

	if *doUninstall && *doBootstrap {
		log.Error("flags --uninstall and --bootstrap can not be used together, quit")
		os.Exit(1)
	}

	if *doUninstall {
		os.Exit(packaging.RunUninstall())
	}

	if *doBootstrap {
		bc := &packaging.BootstrapConfig{
			RunAsUser:                os.Getenv("PGSCV_RUN_AS_USER"),
			SendMetricsURL:           os.Getenv("PGSCV_SEND_METRICS_URL"),
			AutoUpdateURL:            os.Getenv("PGSCV_AUTOUPDATE_URL"),
			APIKey:                   os.Getenv("PGSCV_API_KEY"),
			ProjectID:                os.Getenv("PGSCV_PROJECT_ID"),
			DefaultPostgresPassword:  os.Getenv("PGSCV_PG_PASSWORD"),
			DefaultPgbouncerPassword: os.Getenv("PGSCV_PGB_PASSWORD"),
		}
		os.Exit(packaging.RunBootstrap(bc))
	}

	config, err := pgscv.NewConfig(*configFile)
	if err != nil {
		log.Errorln("parse config failed: ", err)
		os.Exit(1)
	}

	if err := config.Validate(); err != nil {
		log.Errorln("validate config failed: ", err)
		os.Exit(1)
	}

	config.BinaryPath = os.Args[0]
	config.BinaryVersion = fmt.Sprintf("%s %s-%s", gitTag, gitCommit, gitBranch)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	var doExit = make(chan error, 2)
	go func() {
		doExit <- listenSignals()
		cancel()
	}()

	go func() {
		doExit <- pgscv.Start(ctx, config)
		cancel()
	}()

	log.Warnf("shutdown: %s", <-doExit)
}

func listenSignals() error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return fmt.Errorf("got %s", <-c)
}
