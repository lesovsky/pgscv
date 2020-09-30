package collector

import (
	"context"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/nxadm/tail"
	"github.com/prometheus/client_golang/prometheus"
	"io"
	"regexp"
	"sync"
	"time"
)

type messageStore struct {
	messages map[string]float64
	bytes    float64
	mu       sync.RWMutex
}

type postgresLogsCollector struct {
	tail        *tail.Tail
	tailEnabled bool
	store       messageStore
	messages    typedDesc
}

func NewPostgresLogsCollector(constLabels prometheus.Labels) (Collector, error) {
	return &postgresLogsCollector{
		store: messageStore{
			messages: map[string]float64{},
			mu:       sync.RWMutex{},
		},
		messages: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "log", "messages_total"),
				"Total number of log messages written by severity.",
				[]string{"severity"}, constLabels,
			), valueType: prometheus.CounterValue,
		},
	}, nil
}

func (c *postgresLogsCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	if !config.LoggingCollector {
		return nil
	}

	if config.ServerVersionNum < PostgresV10 {
		log.Infof("[postgres log collector]: some system functions are not available, required Postgres 10 or newer")
		return nil
	}

	if !c.tailEnabled {
		go runLogtail(config.ConnString, c)
		c.tailEnabled = true
	}

	c.store.mu.RLock()
	for label, value := range c.store.messages {
		ch <- c.messages.mustNewConstMetric(value, label)
	}
	c.store.mu.RUnlock()

	return nil
}

func runLogtail(conninfo string, c *postgresLogsCollector) {
	logfile, err := queryCurrentLogfile(conninfo)
	if err != nil {
		log.Errorln(err)
		return
	}
	log.Infof("starting log tail for %s", logfile)

	// Start reading from the end, because we don't know the size of the previously written logs
	// and don't know how much resources its processing will take.
	t, err := tail.TailFile(logfile, tail.Config{Location: &tail.SeekInfo{Whence: io.SeekEnd}, Follow: true})
	if err != nil {
		log.Errorln(err)
		return
	}

	defer t.Cleanup()

	c.tail = t

	parser := newLogParser()

	ticker := time.NewTicker(time.Minute)
	lineCh := c.tail.Lines

	for {
		select {
		case <-ticker.C:
			recheck, err := queryCurrentLogfile(conninfo)
			if err != nil {
				log.Errorln(err)
				return
			}

			// do nothing if logfile is not changed
			if recheck == logfile {
				break
			}

			logfile = recheck

			// Otherwise stop listening notify on old log and reopen a new log.
			c.tail.Cleanup()
			err = c.tail.Stop()
			if err != nil {
				log.Errorln(err)
				return
			}

			// Start reading from the start to avoid losing written messages.
			log.Infof("starting log tail for %s", logfile)
			tail2, err := tail.TailFile(logfile, tail.Config{Follow: true})
			if err != nil {
				log.Errorln(err)
				return
			}

			c.tail = tail2
			lineCh = c.tail.Lines

		case line := <-lineCh:
			log.Infoln("lessqq: got the line")
			m, found := parser.parse(line.Text)
			if found {
				c.store.mu.Lock()
				c.store.messages[m]++
				c.store.bytes += float64(len(line.Text))
				c.store.mu.Unlock()
			}
		}
	}
}

//
func queryCurrentLogfile(conninfo string) (string, error) {
	conn, err := store.New(conninfo)
	if err != nil {
		return "", err
	}

	var logfile string
	err = conn.Conn().QueryRow(context.TODO(), "SELECT pg_current_logfile()").Scan(&logfile)
	if err != nil {
		return "", err
	}
	conn.Close()

	return logfile, nil
}

//
type logParser struct {
	re map[string]*regexp.Regexp
}

func newLogParser() *logParser {
	patterns := map[string]string{
		"log":     "LOG:",
		"warning": "WARNING:",
		"error":   "ERROR",
		"fatal":   "FATAL:",
		"panic":   "PANIC",
	}

	re := map[string]*regexp.Regexp{}

	for name, pattern := range patterns {
		re[name] = regexp.MustCompile(pattern)
	}

	return &logParser{
		re: re,
	}
}

//
func (p *logParser) parse(line string) (string, bool) {
	if line == "" {
		return "", false
	}

	for name, re := range p.re {
		if re.MatchString(line) {
			return name, true
		}
	}

	// patterns are not found in the line
	return "", false
}
