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
)

// Current implementation has an issue described here: https://github.com/nxadm/tail/issues/18.
// When attempting to tail previously tailed logfiles, new messages are not coming from the Lines channel.
// At the same time, test Test_runTailLoop works as intended and doesn't show the problem.

type syncKV struct {
	store map[string]float64
	mu    sync.RWMutex
}

type postgresLogsCollector struct {
	updateLogfile  chan string // updateLogfile used for notify tail/collect goroutine when logfile has been changed.
	currentLogfile string      // currentLogfile contains logfile name currently tailed and used for collecting stat.
	totals         syncKV      // totals contains collected stats about total number of log messages.
	messagesTotal  typedDesc
}

// NewPostgresLogsCollector creates new collector for Postgres log messages.
func NewPostgresLogsCollector(constLabels prometheus.Labels) (Collector, error) {
	collector := &postgresLogsCollector{
		updateLogfile: make(chan string),
		totals: syncKV{
			store: map[string]float64{},
			mu:    sync.RWMutex{},
		},
		messagesTotal: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "log", "messages_total"),
				"Total number of log messages written by severity.",
				[]string{"severity"}, constLabels,
			), valueType: prometheus.CounterValue,
		},
	}

	go runTailLoop(collector)

	return collector, nil
}

// runTailLoop accepts logfile names over channel and run tail/collect functions.
func runTailLoop(c *postgresLogsCollector) {
	var ctx context.Context
	var cancel context.CancelFunc
	var wg sync.WaitGroup

	// Run initial tail, it reads logfile from the end.
	ctx, cancel = context.WithCancel(context.Background())
	logfile := <-c.updateLogfile
	wg.Add(1)
	go func() {
		tailCollect(ctx, logfile, true, &wg, c)
	}()

	// Polling logfile changes. When it change, stop initial tail and start tail a new one.
	for logfile := range c.updateLogfile {
		log.Infoln("logfile changed, stopping current tailing")
		cancel()
		wg.Wait()
		ctx, cancel = context.WithCancel(context.Background())

		wg.Add(1)
		logfile := logfile
		go func() {
			tailCollect(ctx, logfile, false, &wg, c)
		}()
	}

	cancel()
}

// tailCollect accepts logfile and tail it. Collected stats are based on received and parsed lines.
func tailCollect(ctx context.Context, logfile string, init bool, wg *sync.WaitGroup, c *postgresLogsCollector) {
	defer wg.Done()

	// When just initialized, start tailing from the end of file - there could be many lines and reading all of them could
	// be expensive. When logfile has been changed (logrotated) start reading from the beginning.
	tailConfig := tail.Config{Follow: true}
	offset := "beginning"
	if init {
		offset = "end"
		tailConfig.Location = &tail.SeekInfo{Whence: io.SeekEnd}
	}

	parser := newLogParser()
	log.Infof("starting tail of %s from the %s", logfile, offset)
	t, err := tail.TailFile(logfile, tailConfig)
	if err != nil {
		log.Errorln(err)
		return
	}

	//log.Infoln("lessqq: waiting for events")
	for {
		select {
		case <-ctx.Done():
			//log.Infoln("lessqq: got ctx.Done, stop parsing")
			t.Cleanup()
			err = t.Stop()
			if err != nil {
				log.Infoln(err)
			}
			return
		case line := <-t.Lines:
			//log.Infoln("lessqq: got the line")
			m, found := parser.parseLogMessage(line.Text)
			if found {
				c.totals.mu.Lock()
				c.totals.store[m]++
				c.totals.mu.Unlock()
			}
		}
	}
}

// Update method generates metrics based on collected log messages.
func (c *postgresLogsCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	if !config.LoggingCollector {
		return nil
	}

	if config.ServerVersionNum < PostgresV10 {
		log.Infof("[postgres log collector]: some system functions are not available, required Postgres 10 or newer")
		return nil
	}

	// Notify log collector goroutine if logfile has been changed.
	logfile, err := queryCurrentLogfile(config.ConnString)
	if err != nil {
		return err
	}

	if logfile != c.currentLogfile {
		c.currentLogfile = logfile
		c.updateLogfile <- logfile
	}

	// Collect metrics.
	c.totals.mu.RLock()
	for label, value := range c.totals.store {
		ch <- c.messagesTotal.mustNewConstMetric(value, label)
	}
	c.totals.mu.RUnlock()

	return nil
}

// queryCurrentLogfile returns path to logfile used by database.
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

// logParser contains set or regexp patterns used for parse log messages.
type logParser struct {
	re map[string]*regexp.Regexp
}

// newLogParser creates a new logParser.
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

// parseLogMessage accepts lines and parse it using patterns from logParser.
func (p *logParser) parseLogMessage(line string) (string, bool) {
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
