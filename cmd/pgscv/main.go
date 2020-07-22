package main

import (
	"context"
	"fmt"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/pgscv"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"os/signal"
	"syscall"
)

var (
	appName, gitCommit, gitBranch string
)

func main() {
	var (
		showVersion = kingpin.Flag("version", "show version and exit").Default().Bool()
		logLevel    = kingpin.Flag("log-level", "set log level: debug, info, warn, error").Default("info").Envar("LOG_LEVEL").String()
		configFile  = kingpin.Flag("config-file", "Path to config file").Default("/etc/pgscv.json").Envar("CONFIG_FILE").String()
	)
	kingpin.Parse()
	log.SetLevel(*logLevel)
	log.SetApplication(appName)

	if *showVersion {
		fmt.Printf("%s %s-%s\n", appName, gitCommit, gitBranch)
		os.Exit(0)
	}

	config, err := pgscv.NewConfig(*configFile)
	if err != nil {
		log.Errorf("Cannot start %s, unable to create config: %s", appName, err)
		os.Exit(1)
	}

	if err := config.Validate(); err != nil {
		log.Errorf("Cannot start %s, unable to validate config: %s", appName, err)
		os.Exit(1)
	}

	config.BinaryVersion = fmt.Sprintf("%s-%s", gitCommit, gitBranch)

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
