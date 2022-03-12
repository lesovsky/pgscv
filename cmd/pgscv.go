package main

import (
	"context"
	"fmt"
	"github.com/lesovsky/pgscv/internal/log"
	"github.com/lesovsky/pgscv/internal/pgscv"
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
		configFile  = kingpin.Flag("config-file", "path to config file").Default("").Envar("PGSCV_CONFIG_FILE").String()
	)
	kingpin.Parse()
	log.SetLevel(*logLevel)
	log.SetApplication(appName)

	if *showVersion {
		fmt.Printf("%s %s %s-%s\n", appName, gitTag, gitCommit, gitBranch)
		os.Exit(0)
	}

	config, err := pgscv.NewConfig(*configFile)
	if err != nil {
		log.Errorln("create config failed: ", err)
		os.Exit(1)
	}

	if err := config.Validate(); err != nil {
		log.Errorln("validate config failed: ", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())

	var doExit = make(chan error, 2)
	go func() {
		doExit <- listenSignals()
		cancel()
	}()

	go func() {
		doExit <- pgscv.Start(ctx, config)
		cancel()
	}()

	log.Warnf("received shutdown signal: '%s'", <-doExit)
}

func listenSignals() error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return fmt.Errorf("%s", <-c)
}
