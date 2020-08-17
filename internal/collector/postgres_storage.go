package collector

import (
	"context"
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
	postgresTempFilesInflightQuery = `SELECT spcname AS tablespace,
    count(*) AS files_total,
    coalesce(sum(size), 0) AS bytes_total,
    coalesce(extract(epoch from clock_timestamp() - min(modification)), 0) AS max_age_seconds
FROM (SELECT spcname,(pg_ls_tmpdir(oid)).* FROM pg_tablespace WHERE spcname != 'pg_global') tablespaces GROUP BY spcname`
)

type postgresStorageCollector struct {
	tempFiles       typedDesc
	tempBytes       typedDesc
	tempFilesMaxAge typedDesc
	dirstats        typedDesc
}

// NewPostgresStorageCollector returns a new Collector exposing various stats related to Postgres storage layer.
// This stats observed using different stats sources.
func NewPostgresStorageCollector(constLabels prometheus.Labels) (Collector, error) {
	return &postgresStorageCollector{
		tempFiles: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "temp_files", "in_flight"),
				"Total number of temporary files processed in flight.",
				[]string{"tablespace"}, constLabels,
			), valueType: prometheus.GaugeValue,
		},
		tempBytes: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "temp_bytes", "in_flight"),
				"Total number bytes occupied by temporary files processed in flight.",
				[]string{"tablespace"}, constLabels,
			), valueType: prometheus.GaugeValue,
		},
		tempFilesMaxAge: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "temp_files", "max_age_seconds"),
				"The age of the oldest temporary file, in seconds.",
				[]string{"tablespace"}, constLabels,
			), valueType: prometheus.GaugeValue,
		},
		dirstats: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "directory_size", "bytes_total"),
				"The size of Postgres server directories of each type, in bytes.",
				[]string{"device", "mountpoint", "path", "type"}, constLabels,
			), valueType: prometheus.GaugeValue,
		},
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresStorageCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	// Some directory listing functions (pg_ls_dir(), pg_ls_waldir(), pg_ls_tmpdir()) are available only since Postgres 10.
	if config.ServerVersionNum < PostgresV10 {
		log.Infoln("[postgres storage collector]: some server-side functions are not available, required Postgres 10 or newer")
		return nil
	}

	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	res, err := conn.Query(postgresTempFilesInflightQuery)
	if err != nil {
		return err
	}

	stats := parsePostgresTempFileInflght(res)

	for _, stat := range stats {
		ch <- c.tempFiles.mustNewConstMetric(stat.tempfiles, stat.tablespace)
		ch <- c.tempBytes.mustNewConstMetric(stat.tempbytes, stat.tablespace)
		ch <- c.tempFilesMaxAge.mustNewConstMetric(stat.tempmaxage, stat.tablespace)
	}

	dirstats, err := newPostgresDirStat(conn, config.DataDirectory)
	if err != nil {
		return err
	}

	ch <- c.dirstats.mustNewConstMetric(dirstats.datadirSizeBytes, dirstats.datadirDevice, dirstats.datadirMountpoint, dirstats.datadirPath, "data")
	ch <- c.dirstats.mustNewConstMetric(dirstats.waldirSizeBytes, dirstats.waldirDevice, dirstats.waldirMountpoint, dirstats.waldirPath, "wal")
	ch <- c.dirstats.mustNewConstMetric(dirstats.logdirSizeBytes, dirstats.logdirDevice, dirstats.logdirMountpoint, dirstats.logdirPath, "log")
	ch <- c.dirstats.mustNewConstMetric(dirstats.tmpfilesSizeBytes, "temp", "temp", "temp", "temp")

	return nil
}

// postgresConflictStat represents per-database recovery conflicts stats based on pg_stat_database_conflicts.
type postgresTempfilesStat struct {
	tablespace string
	tempfiles  float64
	tempbytes  float64
	tempmaxage float64
}

// parsePostgresTempFileInflght parses PGResult, extract data and return struct with stats values.
func parsePostgresTempFileInflght(r *model.PGResult) map[string]postgresTempfilesStat {
	var stats = make(map[string]postgresTempfilesStat)

	// process row by row
	for _, row := range r.Rows {
		stat := postgresTempfilesStat{}

		// collect label values
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "tablespace":
				stat.tablespace = row[i].String
			}
		}

		// Define a map key as a tablespace name.
		tablespaceFQName := stat.tablespace

		// Put stats with labels (but with no data values yet) into stats store.
		stats[tablespaceFQName] = stat

		// fetch data values from columns
		for i, colname := range r.Colnames {
			// skip tablespace column - it's mapped as a label
			if string(colname.Name) == "tablespace" {
				log.Debug("skip label mapped column")
				continue
			}

			// Skip empty (NULL) values.
			if row[i].String == "" {
				log.Debug("got empty (NULL) value, skip")
				continue
			}

			// Get data value and convert it to float64 used by Prometheus.
			v, err := strconv.ParseFloat(row[i].String, 64)
			if err != nil {
				log.Errorf("skip collecting metric: %s", err)
				continue
			}

			// Run column-specific logic
			switch string(colname.Name) {
			case "files_total":
				s := stats[tablespaceFQName]
				s.tempfiles = v
				stats[tablespaceFQName] = s
			case "bytes_total":
				s := stats[tablespaceFQName]
				s.tempbytes = v
				stats[tablespaceFQName] = s
			case "max_age_seconds":
				s := stats[tablespaceFQName]
				s.tempmaxage = v
				stats[tablespaceFQName] = s
			default:
				log.Debugf("unsupported stat column: %s, skip", string(colname.Name))
				continue
			}
		}
	}

	return stats
}

// postgresDirStat represents stats about Postgres system directories
type postgresDirStat struct {
	datadirPath       string
	datadirMountpoint string
	datadirDevice     string
	datadirSizeBytes  float64
	waldirPath        string
	waldirMountpoint  string
	waldirDevice      string
	waldirSizeBytes   float64
	logdirPath        string
	logdirMountpoint  string
	logdirDevice      string
	logdirSizeBytes   float64
	tmpfilesSizeBytes float64
}

// newPostgresDirStat returns sizes of Postgres server directories.
func newPostgresDirStat(conn *store.DB, datadir string) (*postgresDirStat, error) {
	var (
		logdirSizeBytes, waldirSizeBytes, tmpfilesSizeBytes int64
		logdirPath, waldirPath                              string
	)

	datadirSizeBytes, err := getDirectorySize(datadir)
	if err != nil {
		return nil, fmt.Errorf("get data_directory size failed: %s", err)
	}

	err = conn.Conn().
		QueryRow(context.Background(), "SELECT current_setting('log_directory') AS path, sum(size) AS bytes FROM pg_ls_logdir() WHERE current_setting('logging_collector') = 'on'").
		Scan(&logdirPath, &logdirSizeBytes)
	if err != nil {
		return nil, fmt.Errorf("get log directory size failed: %s", err)
	}

	err = conn.Conn().
		QueryRow(context.Background(), "SELECT current_setting('data_directory')||'/pg_wal' AS path, sum(size) AS bytes FROM pg_ls_waldir()").
		Scan(&waldirPath, &waldirSizeBytes)
	if err != nil {
		return nil, fmt.Errorf("get WAL directory size failed: %s", err)
	}

	err = conn.Conn().
		QueryRow(context.Background(), "SELECT coalesce(sum(size), 0) AS total_bytes FROM (SELECT (pg_ls_tmpdir(oid)).size FROM pg_tablespace WHERE spcname != 'pg_global') tablespaces").
		Scan(&tmpfilesSizeBytes)
	if err != nil {
		return nil, fmt.Errorf("get total size of temp files failed: %s", err)
	}

	// Get directories mountpoints.
	mounts, err := getMountpoints()
	if err != nil {
		return nil, fmt.Errorf("get mountpoints failed: %s", err)
	}

	// Find mountpoint and device for DATA directory.
	datadirMountpoint, device, err := findMountpoint(mounts, datadir)
	if err != nil {
		return nil, fmt.Errorf("find data directory mountpoint failed: %s", err)
	}
	datadirDevice := truncateDeviceName(device)

	// Find mountpoint and device for WAL directory.
	waldirMountpoint, device, err := findMountpoint(mounts, waldirPath)
	if err != nil {
		return nil, fmt.Errorf("find WAL directory mountpoint failed: %s", err)
	}
	waldirDevice := truncateDeviceName(device)

	// Find mountpoint and device for LOG directory.
	logdirMountpoint, device, err := findMountpoint(mounts, logdirPath)
	if err != nil {
		return nil, fmt.Errorf("find log directory mountpoint failed: %s", err)
	}
	logdirDevice := truncateDeviceName(device)

	// Return stats and directories properties.
	return &postgresDirStat{
		datadirPath:       datadir,
		datadirMountpoint: datadirMountpoint,
		datadirDevice:     datadirDevice,
		datadirSizeBytes:  float64(datadirSizeBytes),
		waldirPath:        waldirPath,
		waldirMountpoint:  waldirMountpoint,
		waldirDevice:      waldirDevice,
		waldirSizeBytes:   float64(waldirSizeBytes),
		logdirPath:        logdirPath,
		logdirMountpoint:  logdirMountpoint,
		logdirDevice:      logdirDevice,
		logdirSizeBytes:   float64(logdirSizeBytes),
		tmpfilesSizeBytes: float64(tmpfilesSizeBytes),
	}, nil
}

// getDirectorySize walk through directory tree, calculate sizes and return total size of the directory.
func getDirectorySize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		// ignore ENOENT errors, they don't affect overall result.
		if err != nil {
			if strings.HasSuffix(err.Error(), "no such file or directory") {
				return nil
			}
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}

// findMountpoint checks path in the list of passed mountpoints.
func findMountpoint(mounts []mount, path string) (string, string, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return "", "", err
	}

	// If it is a symlink dereference it and try to find mountpoint again.
	if fi.Mode()&os.ModeSymlink != 0 {
		resolved, err := os.Readlink(path)
		if err != nil {
			return "", "", err
		}
		return findMountpoint(mounts, resolved)
	}

	// Check path in a list of all mounts.
	for _, m := range mounts {
		if m.mountpoint == path {
			log.Debugf("mountpoint found: %s\n", path)
			return path, m.device, nil
		}
	}

	// If path is not in mounts list, truncate path by one directory and try again.
	parts := strings.Split(path, "/")
	if len(parts) == 0 {
		return "", "", fmt.Errorf("mountpoint not found")
	}

	path = strings.Join(parts[0:len(parts)-1], "/")
	if path == "" {
		path = "/"
	}

	return findMountpoint(mounts, path)
}

// getMountpoints opens /proc/mounts file and run parser.
func getMountpoints() ([]mount, error) {
	file, err := os.Open("/proc/mounts")
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	return parseProcMounts(file, nil)
}
