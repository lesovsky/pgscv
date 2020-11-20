package collector

import (
	"bufio"
	"fmt"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	// admin console query used for retrieving settings
	settingsQuery = "SHOW CONFIG"
)

type pgbouncerSettingsCollector struct {
	settings   typedDesc
	dbSettings typedDesc
	poolSize   typedDesc
}

// NewPgbouncerSettingsCollector returns a new Collector exposing pgbouncer configuration.
// For details see https://www.pgbouncer.org/usage.html#show-config.
func NewPgbouncerSettingsCollector(constLabels prometheus.Labels) (Collector, error) {
	return &pgbouncerSettingsCollector{
		settings: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("pgbouncer", "service", "settings"),
				"Labeled information about Pgbouncer configuration settings.",
				[]string{"name", "setting"}, constLabels,
			),
			valueType: prometheus.GaugeValue,
		},
		dbSettings: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("pgbouncer", "service", "database_settings"),
				"Labeled information about Pgbouncer's per-database configuration settings.",
				[]string{"database", "mode", "size"}, constLabels,
			),
			valueType: prometheus.GaugeValue,
		},
		poolSize: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("pgbouncer", "service", "database_pool_size"),
				"Maximum size of pools for the database.",
				[]string{"database"}, constLabels,
			),
			valueType: prometheus.GaugeValue,
		},
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *pgbouncerSettingsCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	res, err := conn.Query(settingsQuery)
	if err != nil {
		return err
	}

	settings := parsePgbouncerSettings(res)

	for k, v := range settings {
		// If value could be converted to numeric, send it as value. For string values use "1".
		if f, err := strconv.ParseFloat(v, 64); err != nil {
			ch <- c.settings.mustNewConstMetric(1, k, v)
		} else {
			ch <- c.settings.mustNewConstMetric(f, k, v)
		}
	}

	if conffile, ok := settings["conffile"]; ok {
		dbSettings, err := getPerDatabaseSettings(
			conffile,
			map[string]string{
				"pool_mode":         settings["pool_mode"],
				"default_pool_size": settings["default_pool_size"],
			},
		)
		if err != nil {
			return err
		}

		for _, p := range dbSettings {
			ch <- c.dbSettings.mustNewConstMetric(1, p.name, p.mode, p.size)

			if f, err := strconv.ParseFloat(p.size, 64); err != nil {
				log.Warnf("failed to parse pool_size: '%s'; skip", p.size)
				continue
			} else {
				ch <- c.poolSize.mustNewConstMetric(f, p.name)
			}
		}
	}

	return nil
}

// parsePgbouncerSettings parses content of 'SHOW CONFIG' and return map with parsed settings.
func parsePgbouncerSettings(r *model.PGResult) map[string]string {
	settings := make(map[string]string)

	for _, row := range r.Rows {
		if len(row) < 2 {
			log.Warnln("invalid number of columns, skip")
			continue
		}

		// Important: order of items depends on format of returned columns in SHOW CONFIG.
		key, value := row[0].String, row[1].String
		settings[key] = value
	}

	return settings
}

// dbSettings describes per-database settings specified inside [database] section of pgbouncer config file.
type dbSettings struct {
	name string
	mode string // value of 'pool_mode'
	size string // value of 'pool_size'
}

// getPerDatabaseSettings parses [databases] section in pgbouncer config file and returns per-pool settings.
func getPerDatabaseSettings(filename string, defaults map[string]string) ([]dbSettings, error) {
	var settings []dbSettings

	file, err := os.Open(filepath.Clean(filename))
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	var databaseSection bool // databaseSection defines that scanner is inside [database] section
	var scanner = bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()

		// Looking for 'databases' section which contains per pool settings. Skip all lines until "[databases]" line found.
		// Being in 'databases' parse all lines and collect per pool settings until next section starts.

		if databaseSection {
			// Skip comments and empty lines
			if strings.HasPrefix(line, ";") || line == "" {
				continue
			}

			// Check for the beginning of the next section.
			if strings.HasPrefix(line, "[") {
				databaseSection = false
				continue
			}

			// parse line and extract necessary settings.
			s, err := parseDatabaseSettingsLine(line)
			if err != nil {
				log.Warnln(err)
				continue
			}

			// set defaults if necessary options were not specified.
			if s.size == "" {
				s.size = defaults["default_pool_size"]
			}
			if s.mode == "" {
				s.mode = defaults["pool_mode"]
			}

			// append pool settings to store
			settings = append(settings, s)
		} else {
			if strings.TrimSpace(line) == "[databases]" {
				databaseSection = true
				continue
			}
		}
	}

	return settings, nil
}

// parseDatabaseSettingsLine parses line with database settings and return dbSettings struct.
func parseDatabaseSettingsLine(line string) (dbSettings, error) {
	var s dbSettings

	parts := strings.SplitN(line, "=", 2)
	if len(parts) < 2 {
		return s, fmt.Errorf("parse [databases] section warning, bad content: '%s', skip", line)
	}

	s.name = strings.TrimSpace(parts[0])

	// parsing database options
	subparts := strings.Split(strings.TrimSpace(parts[1]), " ")
	for _, p := range subparts {
		values := strings.Split(p, "=")
		if len(values) < 2 {
			log.Warnf("parse database settings warning, bad content: '%s', skip", p)
			continue
		}

		switch strings.TrimSpace(values[0]) {
		case "pool_mode":
			s.mode = strings.TrimSpace(values[1])
		case "pool_size":
			s.size = strings.TrimSpace(values[1])
		}
	}

	return s, nil
}
