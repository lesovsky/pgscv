package collector

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// postgresSettingsCollector defines metric descriptors and stats store.
type postgresSettingsCollector struct {
	settings typedDesc
	files    typedDesc
}

// NewPostgresSettingsCollector returns a new Collector exposing postgres settings stats.
// For details see https://www.postgresql.org/docs/current/view-pg-settings.html
// and https://www.postgresql.org/docs/current/view-pg-file-settings.html
func NewPostgresSettingsCollector(constLabels prometheus.Labels) (Collector, error) {
	return &postgresSettingsCollector{
		settings: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "service", "settings_info"),
				"Labeled information about Postgres configuration settings.",
				[]string{"name", "setting", "unit", "vartype", "source"}, constLabels,
			),
			valueType: prometheus.GaugeValue,
		},
		files: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "service", "files_info"),
				"Labeled information about Postgres system files.",
				[]string{"guc", "mode", "path"}, constLabels,
			),
			valueType: prometheus.GaugeValue,
		},
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresSettingsCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	// For complete list of displayable names of GUC's sources types check guc.c (see GucSource_Names[]).
	query := "SELECT name, setting, unit, vartype FROM pg_show_all_settings() " +
		"WHERE source IN ('default','configuration file','override','environment variable','command line','global')"
	res, err := conn.Query(query)
	if err != nil {
		return err
	}

	settings := parsePostgresSettings(res)

	for _, s := range settings {
		ch <- c.settings.mustNewConstMetric(s.value, s.name, s.setting, s.unit, s.vartype, "main")
	}

	query = `SELECT name, setting FROM pg_show_all_settings() WHERE name IN ('config_file','hba_file','ident_file','data_directory')`
	res, err = conn.Query(query)
	if err != nil {
		return err
	}

	files := parsePostgresFiles(res)

	for _, f := range files {
		ch <- c.files.mustNewConstMetric(1, f.guc, f.mode, f.path)
	}

	return nil
}

// postgresSetting is per-setting store for metrics related to postgres settings.
type postgresSetting struct {
	name    string  // pg_settings.name
	setting string  // pg_settings.setting
	unit    string  // pg_settings.unit
	vartype string  // pg_settings.vartype
	value   float64 // float64 representation of pg_settings.settings (if 'vartype' is bool, numeric or real)
}

// parsePostgresSettings parses PGResult and returns structs with settings data.
func parsePostgresSettings(r *model.PGResult) []postgresSetting {
	log.Debug("parse postgres settings")

	var settings []postgresSetting

	for _, row := range r.Rows {
		if len(row) != 4 {
			log.Warnln("invalid input, wrong number of columns; skip")
			continue
		}

		// Important: order of items depends on order of columns in SELECT statement.
		n, s, u, v := row[0].String, row[1].String, row[2].String, row[3].String
		setting, err := newPostgresSetting(n, s, u, v)
		if err != nil {
			log.Warnf("normalize setting (name=%s, setting=%s, unit=%s, vartype=%s) failed: %s; skip", n, s, u, v, err.Error())
			continue
		}

		// Append setting to store.
		settings = append(settings, setting)
	}

	return settings
}

// newPostgresSetting reads settings related values and create new postgresSetting struct.
func newPostgresSetting(name, setting, unit, vartype string) (postgresSetting, error) {
	var value float64

	switch vartype {
	case "enum", "string":
		// In case of 'enum' or 'string' vartypes we could do nothing and return all values as is.
		return postgresSetting{
			name:    name,
			unit:    unit,
			vartype: vartype,
			setting: setting,
			value:   0,
		}, nil
	case "bool":
		// In case of 'bool' vartype, also return all values as is and cast setting value to float64.
		switch setting {
		case "off":
			value = 0
		case "on":
			value = 1
		default:
			return postgresSetting{}, fmt.Errorf("invalid bool value: '%s'", setting)
		}

		return postgresSetting{
			name:    name,
			unit:    unit,
			vartype: vartype,
			setting: setting,
			value:   value,
		}, nil
	case "integer", "real":
		// Parse the unit and cast it to base unit with factor.
		factor, unit, err := parseUnit(unit)
		if err != nil {
			return postgresSetting{}, err
		}

		// Parse setting value to float64
		v, err := strconv.ParseFloat(setting, 64)
		if err != nil {
			return postgresSetting{}, err
		}

		// Apply factor only to positive setting values - negative values considered as a specials (eg. old_snapshot_threshold).
		if v >= 0 {
			v = v * factor
		}

		// Remove fractional part for 'integer' vartype, and truncate to '3' for 'real'. For 'integers' less than 1 keep fractional part.
		if vartype == "integer" && v >= 1 {
			setting = strconv.FormatFloat(v, 'f', 0, 64)
		} else {
			// Converted value can look like 1.500, 1.000 or 0.000. They should be converted to 1.5, 1, 0.
			// Don't combine trailing zeroes and dot in single TrimRight() - it leads to converting '100.000' to '1'.

			// First remove all trailing zeroes.
			setting = strings.TrimRight(strconv.FormatFloat(v, 'f', 5, 64), "0")
			// Next remove trailing dot in case if number was like x.000
			setting = strings.TrimRight(setting, ".")
			// if setting become empty, make it zero
			if setting == "" {
				setting = "0"
			}
		}

		return postgresSetting{
			name:    name,
			unit:    unit,
			vartype: vartype,
			setting: setting,
			value:   v,
		}, nil
	default:
		return postgresSetting{}, fmt.Errorf("unknown vartype: '%s'", vartype)
	}
}

// postgresFile describes various info about Postgres system files.
type postgresFile struct {
	path string
	mode string
	guc  string
}

// parsePostgresFiles parses query result and produces slice with info about Postgres system files.
func parsePostgresFiles(r *model.PGResult) []postgresFile {
	log.Debug("parse postgres files")

	var files []postgresFile

	for _, row := range r.Rows {
		if len(row) != 2 {
			log.Warnln("invalid input, wrong number of columns, skip")
			continue
		}

		// Important: order of items depends on order of columns in SELECT statement.
		guc, path := row[0].String, row[1].String
		fi, err := os.Stat(path)
		if err != nil {
			log.Warnf("stat %s failed: %s; skip", path, err)
		}

		mode := fmt.Sprintf("%04o", fi.Mode().Perm())

		file := postgresFile{
			path: path,
			mode: mode,
			guc:  guc,
		}

		// Append file to store.
		files = append(files, file)
	}

	return files
}

// parseUnit parses pg_settings.unit value and normalize it to factor and base unit (bytes or seconds).
// In case of errors return 1 as factor (to avoid zero multiplication) and empty unit and struct.
func parseUnit(unit string) (float64, string, error) {
	if unit == "" {
		return 1, "", nil
	}

	re, err := regexp.Compile(`^(?i)([0-9]*)([a-z]+)$`)
	if err != nil {
		return 1, "", err
	}

	match := re.FindStringSubmatch(unit)

	if len(match) != 3 {
		return 1, "", fmt.Errorf("invalid number of values: %d", len(match))
	}

	var factor float64 = 1
	var suffix = match[2]

	// Recalculate factor if it is specified explicitly.
	if match[1] != "" {
		factor, err = strconv.ParseFloat(match[1], 64)
		if err != nil {
			return 1, "", err
		}
	}

	// Recalculate factor accordingly to suffix.
	switch suffix {
	case "B":
		return factor * 1, "bytes", nil
	case "kB":
		return factor * 1024, "bytes", nil
	case "MB":
		return factor * 1024 * 1024, "bytes", nil
	case "GB":
		return factor * 1024 * 1024 * 1024, "bytes", nil
	case "TB":
		return factor * 1024 * 1024 * 1024 * 1024, "bytes", nil
	case "ms":
		return factor * 0.001, "seconds", nil
	case "s":
		return factor * 1, "seconds", nil
	case "min":
		return factor * 60, "seconds", nil
	case "h":
		return factor * 60 * 60, "seconds", nil
	case "d":
		return factor * 60 * 60 * 24, "seconds", nil
	default:
		return 1, "", fmt.Errorf("unknown suffix: %s", suffix)
	}
}
