package collector

import (
	"bufio"
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/lesovsky/pgscv/internal/log"
	"github.com/lesovsky/pgscv/internal/model"
	"github.com/lesovsky/pgscv/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const (
	settingsQuery = "SHOW CONFIG"
	versionQuery  = "SHOW VERSION"
)

type pgbouncerSettingsCollector struct {
	version    typedDesc
	settings   typedDesc
	dbSettings typedDesc
	poolSize   typedDesc
}

// NewPgbouncerSettingsCollector returns a new Collector exposing pgbouncer configuration.
// For details see https://www.pgbouncer.org/usage.html#show-config.
func NewPgbouncerSettingsCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	return &pgbouncerSettingsCollector{
		version: newBuiltinTypedDesc(
			descOpts{"pgbouncer", "", "version", "Numeric representation of Pgbouncer version.", 0},
			prometheus.GaugeValue,
			[]string{"version"}, constLabels,
			settings.Filters,
		),
		settings: newBuiltinTypedDesc(
			descOpts{"pgbouncer", "service", "settings_info", "Labeled information about Pgbouncer configuration settings.", 0},
			prometheus.GaugeValue,
			[]string{"name", "setting"}, constLabels,
			settings.Filters,
		),
		dbSettings: newBuiltinTypedDesc(
			descOpts{"pgbouncer", "service", "database_settings_info", "Labeled information about Pgbouncer's per-database configuration settings.", 0},
			prometheus.GaugeValue,
			[]string{"database", "mode", "size"}, constLabels,
			settings.Filters,
		),
		poolSize: newBuiltinTypedDesc(
			descOpts{"pgbouncer", "service", "database_pool_size", "Maximum size of pools for the database.", 0},
			prometheus.GaugeValue,
			[]string{"database"}, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *pgbouncerSettingsCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Pgbouncers before 1.12 return version as a NOTICE message (not as normal row) and it seems
	// there is no way to extract version string. Query the version, if zero value is returned it
	// means there is an old Pgbouncer is answered. Just skip collecting version metric and continue.

	version, versionStr, err := queryPgbouncerVersion(conn)
	if err != nil {
		return err
	}
	if version != 0 {
		ch <- c.version.newConstMetric(float64(version), versionStr)
	}

	// Query pgbouncer settings.

	res, err := conn.Query(settingsQuery)
	if err != nil {
		return err
	}

	settings := parsePgbouncerSettings(res)

	for k, v := range settings {
		// If value could be converted to numeric, send it as value. For string values use "1".
		if f, err := strconv.ParseFloat(v, 64); err != nil {
			ch <- c.settings.newConstMetric(1, k, v)
		} else {
			ch <- c.settings.newConstMetric(f, k, v)
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
			ch <- c.dbSettings.newConstMetric(1, p.name, p.mode, p.size)

			if f, err := strconv.ParseFloat(p.size, 64); err != nil {
				log.Warnf("invalid input, parse '%s' failed: %s; skip", p.size, err)
				continue
			} else {
				ch <- c.poolSize.newConstMetric(f, p.name)
			}
		}
	}

	return nil
}

// queryPgbouncerVersion queries version info from Pgbouncer and return numeric and string version representation.
func queryPgbouncerVersion(conn *store.DB) (int, string, error) {
	var versionStr string
	err := conn.Conn().QueryRow(context.Background(), versionQuery).Scan(&versionStr)
	if err != nil {
		// Pgbouncer before 1.12 returns version string as a NOTICE, and it seems there is no way to extract
		// message text from the NOTICE. Return zero value and nil as error.
		// Pgbouncer changelog: https://www.pgbouncer.org/changelog.html#pgbouncer-112x
		if err == pgx.ErrNoRows {
			return 0, "", nil
		}

		return 0, "", fmt.Errorf("read version string failed: %s", err)
	}

	re := regexp.MustCompile(`\d+\.\d+\.\d+`)
	versionStr = re.FindString(versionStr)

	version, err := semverStringToInt(versionStr)
	if err != nil {
		return 0, "", fmt.Errorf("parse version string '%s' failed: %s", versionStr, err)
	}

	return version, versionStr, nil
}

// parsePgbouncerSettings parses content of 'SHOW CONFIG' and return map with parsed settings.
func parsePgbouncerSettings(r *model.PGResult) map[string]string {
	log.Debug("parse pgbouncer settings")

	settings := make(map[string]string)

	for _, row := range r.Rows {
		if len(row) < 2 {
			log.Warnln("invalid input: too few values; skip")
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
	log.Debug("parse pgbouncer database settings")

	var s dbSettings

	parts := strings.SplitN(line, "=", 2)
	if len(parts) < 2 {
		return s, fmt.Errorf("invalid input, '%s': too few values", line)
	}

	s.name = strings.TrimSpace(parts[0])

	// parsing database options
	subparts := strings.Split(strings.TrimSpace(parts[1]), " ")
	for _, p := range subparts {
		values := strings.Split(p, "=")
		if len(values) < 2 {
			log.Warnf("invalid input, '%s': too few values; skip", p)
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
