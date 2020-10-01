package collector

import (
	"context"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/nxadm/tail"
	"github.com/prometheus/client_golang/prometheus"
	"io"
	"regexp"
	"strings"
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
	updateLogfile   chan string // updateLogfile used for notify tail/collect goroutine when logfile has been changed.
	currentLogfile  string      // currentLogfile contains logfile name currently tailed and used for collecting stat.
	totals          syncKV      // totals contains collected stats about total number of log messages.
	panics          syncKV      // panics contains all collected messages with PANIC severity.
	fatals          syncKV      // fatals contains all collected messages with FATAL severity.
	errors          syncKV      // errors contains all collected messages with ERROR severity.
	warnings        syncKV      // warnings contains all collected messages with WARNING severity.
	messagesTotal   typedDesc
	panicMessages   typedDesc
	fatalMessages   typedDesc
	errorMessages   typedDesc
	warningMessages typedDesc
}

// NewPostgresLogsCollector creates new collector for Postgres log messages.
func NewPostgresLogsCollector(constLabels prometheus.Labels) (Collector, error) {
	collector := &postgresLogsCollector{
		updateLogfile: make(chan string),
		totals: syncKV{
			store: map[string]float64{},
			mu:    sync.RWMutex{},
		},
		panics: syncKV{
			store: map[string]float64{},
			mu:    sync.RWMutex{},
		},
		fatals: syncKV{
			store: map[string]float64{},
			mu:    sync.RWMutex{},
		},
		errors: syncKV{
			store: map[string]float64{},
			mu:    sync.RWMutex{},
		},
		warnings: syncKV{
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
		panicMessages: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "log", "panic_messages_total"),
				"Total number of PANIC log messages written.",
				[]string{"msg"}, constLabels,
			), valueType: prometheus.CounterValue,
		},
		fatalMessages: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "log", "fatal_messages_total"),
				"Total number of FATAL log messages written.",
				[]string{"msg"}, constLabels,
			), valueType: prometheus.CounterValue,
		},
		errorMessages: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "log", "error_messages_total"),
				"Total number of ERROR log messages written.",
				[]string{"msg"}, constLabels,
			), valueType: prometheus.CounterValue,
		},
		warningMessages: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "log", "warning_messages_total"),
				"Total number of WARNING log messages written.",
				[]string{"msg"}, constLabels,
			), valueType: prometheus.CounterValue,
		},
	}

	go runTailLoop(collector)

	return collector, nil
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

	// Read collected stats and create metrics.

	// Totals.
	c.totals.mu.RLock()
	for label, value := range c.totals.store {
		ch <- c.messagesTotal.mustNewConstMetric(value, label)
	}
	c.totals.mu.RUnlock()

	// PANIC messages.
	c.panics.mu.RLock()
	for msg, value := range c.panics.store {
		ch <- c.panicMessages.mustNewConstMetric(value, msg)
	}
	c.panics.mu.RUnlock()

	// FATAL messages.
	c.fatals.mu.RLock()
	for msg, value := range c.fatals.store {
		ch <- c.fatalMessages.mustNewConstMetric(value, msg)
	}
	c.fatals.mu.RUnlock()

	// ERROR messages.
	c.errors.mu.RLock()
	for msg, value := range c.errors.store {
		ch <- c.errorMessages.mustNewConstMetric(value, msg)
	}
	c.errors.mu.RUnlock()

	// WARNING messages.
	c.warnings.mu.RLock()
	for msg, value := range c.warnings.store {
		ch <- c.warningMessages.mustNewConstMetric(value, msg)
	}
	c.warnings.mu.RUnlock()

	return nil
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
			parser.updateMessagesStats(line.Text, c)
		}
	}
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
	reSeverity  map[string]*regexp.Regexp // regexp to determine messages severity.
	reExtract   *regexp.Regexp            // regexp for extracting exact messages from the whole line (drop log_line_prefix stuff).
	reNormalize []*regexp.Regexp          // regexp for normalizing log message.
}

// newLogParser creates a new logParser with necessary compiled regexp objects.
func newLogParser() *logParser {
	severityPatterns := map[string]string{
		"log":     `\s?LOG:\s+`,
		"warning": `\s?WARNING:\s+`,
		"error":   `\s?ERROR:\s+`,
		"fatal":   `\s?FATAL:\s+`,
		"panic":   `\s?PANIC:\s+`,
	}

	normalizePatterns := []string{
		`(\s+\d+\s?)`,
		`(\s+".+?"\s?)`,
	}

	p := &logParser{
		reSeverity:  map[string]*regexp.Regexp{},
		reNormalize: make([]*regexp.Regexp, len(normalizePatterns)),
	}

	for name, pattern := range severityPatterns {
		p.reSeverity[name] = regexp.MustCompile(pattern)
	}

	p.reExtract = regexp.MustCompile(`\s?(PANIC|FATAL|ERROR|WARNING):\s+(.+)`)

	for i, pattern := range normalizePatterns {
		p.reNormalize[i] = regexp.MustCompile(pattern)
	}

	return p
}

// updateMessagesStats process the message string, parse and update stats.
func (p *logParser) updateMessagesStats(line string, c *postgresLogsCollector) {
	//log.Infoln("lessqq: got the line")
	m, found := p.parseMessageSeverity(line)
	if !found {
		return
	}

	// Update totals.
	c.totals.mu.Lock()
	c.totals.store[m]++
	c.totals.mu.Unlock()

	if m == "log" {
		return
	}

	// Message with severity higher than LOG, normalize them and update.
	normalized := p.normalizeMessage(line)
	switch m {
	case "panic":
		c.panics.mu.Lock()
		c.panics.store[normalized]++
		c.panics.mu.Unlock()
	case "fatal":
		c.fatals.mu.Lock()
		c.fatals.store[normalized]++
		c.fatals.mu.Unlock()
	case "error":
		c.errors.mu.Lock()
		c.errors.store[normalized]++
		c.errors.mu.Unlock()
	case "warning":
		c.warnings.mu.Lock()
		c.warnings.store[normalized]++
		c.warnings.mu.Unlock()
	}
}

// parseMessageSeverity accepts lines and parse it using patterns from logParser.
func (p *logParser) parseMessageSeverity(line string) (string, bool) {
	if line == "" {
		return "", false
	}

	for name, re := range p.reSeverity {
		if re.MatchString(line) {
			return name, true
		}
	}

	// Patterns are not found in the line.
	return "", false
}

// normalizeMessage used for normalizing log messages and removing unique elements like names or ids.
func (p *logParser) normalizeMessage(message string) string {
	parts := p.reExtract.FindStringSubmatch(message)
	if len(parts) < 2 {
		return ""
	}

	message = parts[2]

	for _, re := range p.reNormalize {
		message = strings.TrimSpace(re.ReplaceAllString(message, " ? "))
	}

	return message
}
