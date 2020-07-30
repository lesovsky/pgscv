package collector

import (
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
	"strings"
)

const (
	// systemCatalogSizeQuery aggregates all tables sizes from pg_stat_sys_tables where tables are in 'pg_catalog' schema.
	systemCatalogSizeQuery = `SELECT sum(pg_total_relation_size(relname::regclass)) AS bytes FROM pg_stat_sys_tables WHERE schemaname = 'pg_catalog'`

	// schemaNonPKTablesQuery searches all tables with absent PRIMARY or UNIQUE key constraints.
	schemaNonPKTablesQuery = `SELECT t.nspname AS schemaname, t.relname AS relname
FROM (SELECT c.oid, c.relname, n.nspname
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog', 'information_schema')
      ) AS t
LEFT OUTER JOIN pg_constraint c ON c.contype in ('p', 'u') AND c.conrelid = t.oid WHERE c.conname IS NULL`
)

// postgresSchemaCollector defines metric descriptors and stats store.
type postgresSchemaCollector struct {
	syscatalog  typedDesc
	nonpktables typedDesc
}

// NewPostgresSchemaCollector returns a new Collector exposing postgres schema stats. Stats are based on different
// sources inside system catalog.
func NewPostgresSchemaCollector(constLabels prometheus.Labels) (Collector, error) {
	return &postgresSchemaCollector{
		syscatalog: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "schema", "system_catalog_bytes_total"),
				"Total number of bytes occupied by system catalog.",
				[]string{"datname"}, constLabels,
			),
			valueType: prometheus.GaugeValue,
		},
		nonpktables: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "schema", "non_pk_tables_total"),
				"labeled information about tables with no promary or unique key constraints.",
				[]string{"datname", "schemaname", "relname"}, constLabels,
			),
			valueType: prometheus.GaugeValue,
		},
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresSchemaCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}

	databases, err := listDatabases(conn)
	if err != nil {
		return err
	}

	conn.Close()

	pgconfig, err := pgx.ParseConfig(config.ConnString)
	if err != nil {
		return err
	}

	// walk through all databases, connect to it and collect schema-specific stats
	for _, d := range databases {
		pgconfig.Database = d
		conn, err := store.NewWithConfig(pgconfig)
		if err != nil {
			return err
		}

		// 1. get system catalog size in bytes.
		if size := getSystemCatalogSize(conn); size > 0 {
			ch <- c.syscatalog.mustNewConstMetric(size, d)
		}

		// 2. pg_schema_non_pk_table
		// tables are the slice of strings where each string is the table's FQN in following format: schemaname/relname
		tables := getSchemaNonPKTables(conn)

		for _, t := range tables {
			parts := strings.Split(t, "/")
			if len(parts) != 2 {
				log.Warnf("incorrect table FQ name: %s; skip", t)
				continue
			}
			ch <- c.nonpktables.mustNewConstMetric(1, d, parts[0], parts[1])
		}

		// 3. pg_schema_invalid_index

		// 4. pg_schema_non_indexed_fkey

		// 5. pg_schema_redundant_index

		// 6. pg_schema_sequence_fullness

		// 7. pg_schema_fkey_columns_mismatch
	}

	return nil
}

// getSystemCatalogSize returns size of system catalog in bytes
func getSystemCatalogSize(conn *store.DB) float64 {
	var size int64 = 0
	if err := conn.Conn().QueryRow(context.Background(), systemCatalogSizeQuery).Scan(&size); err != nil {
		log.Errorf("get system catalog size failed: %s; skip", err)
	}
	return float64(size)
}

// collectSchemaNonPKTables collect non-pk-tables in the database and send metrics if such tables found found.
func getSchemaNonPKTables(conn *store.DB) []string {
	rows, err := conn.Conn().Query(context.Background(), schemaNonPKTablesQuery)
	if err != nil {
		log.Errorf("collect non-pl tables in database %s failed: %s; skip", conn.Conn().Config().Database, err)
		return nil
	}

	var tables = []string{}
	var schemaname, relname, tableFQName string

	for rows.Next() {
		err := rows.Scan(&schemaname, &relname)
		if err != nil {
			log.Errorf("row scan failed when collecting non-pk tables: %s; skip", err)
			continue
		}

		tableFQName = schemaname + "/" + relname
		tables = append(tables, tableFQName)
	}

	rows.Close()

	return tables
}
