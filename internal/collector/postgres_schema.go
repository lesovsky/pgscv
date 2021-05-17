package collector

import (
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"golang.org/x/net/context"
	"strings"
)

// postgresSchemaCollector defines metric descriptors and stats store.
type postgresSchemaCollector struct {
	syscatalog   typedDesc
	nonpktables  typedDesc
	invalididx   typedDesc
	nonidxfkey   typedDesc
	redundantidx typedDesc
	sequences    typedDesc
	difftypefkey typedDesc
}

// NewPostgresSchemaCollector returns a new Collector exposing postgres schema stats. Stats are based on different
// sources inside system catalog.
func NewPostgresSchemasCollector(constLabels prometheus.Labels, _ model.CollectorSettings) (Collector, error) {
	return &postgresSchemaCollector{
		syscatalog: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "schema", "system_catalog_bytes"),
				"Number of bytes occupied by system catalog.",
				[]string{"database"}, constLabels,
			),
			valueType: prometheus.GaugeValue,
		},
		nonpktables: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "schema", "non_pk_tables"),
				"Labeled information about tables with no primary or unique key constraints.",
				[]string{"database", "schema", "table"}, constLabels,
			),
			valueType: prometheus.GaugeValue,
		},
		invalididx: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "schema", "invalid_indexes_bytes"),
				"Number of bytes occupied by invalid indexes.",
				[]string{"database", "schema", "table", "index"}, constLabels,
			),
			valueType: prometheus.GaugeValue,
		},
		nonidxfkey: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "schema", "non_indexed_fkeys"),
				"Number of non-indexed FOREIGN key constraints.",
				[]string{"database", "schema", "table", "columns", "constraint", "referenced"}, constLabels,
			),
			valueType: prometheus.GaugeValue,
		},
		redundantidx: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "schema", "redundant_indexes_bytes"),
				"Number of bytes occupied by redundant indexes.",
				[]string{"database", "schema", "table", "index", "indexdef", "redundantdef"}, constLabels,
			),
			valueType: prometheus.GaugeValue,
		},
		sequences: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "schema", "sequence_exhaustion_ratio"),
				"Sequences usage percentage accordingly to attached column, in percent.",
				[]string{"database", "schema", "sequence"}, constLabels,
			),
			valueType: prometheus.GaugeValue,
		},
		difftypefkey: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "schema", "mistyped_fkeys"),
				"Number of foreign key constraints with different data type.",
				[]string{"database", "schema", "table", "column", "refschema", "reftable", "refcolumn"}, constLabels,
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
		// Skip database if not matched to allowed.
		if config.DatabasesRE != nil && !config.DatabasesRE.MatchString(d) {
			continue
		}

		pgconfig.Database = d
		conn, err := store.NewWithConfig(pgconfig)
		if err != nil {
			return err
		}

		// 1. get system catalog size in bytes.
		collectSystemCatalogSize(conn, ch, c.syscatalog)

		// 2. collect metrics related to tables with no primary/unique key constraints.
		collectSchemaNonPKTables(conn, ch, c.nonpktables)

		// Functions below uses queries with casting to regnamespace data type, which is introduced in Postgres 9.5.
		if config.ServerVersionNum < PostgresV95 {
			log.Debugln("[postgres schema collector]: some system data types are not available, required Postgres 9.5 or newer")
			conn.Close()
			continue
		}

		// 3. collect metrics related to invalid indexes.
		collectSchemaInvalidIndexes(conn, ch, c.invalididx)

		// 4. collect metrics related to non indexed foreign key constraints.
		collectSchemaNonIndexedFK(conn, ch, c.nonidxfkey)

		// 5. collect metric related to redundant indexes.
		collectSchemaRedundantIndexes(conn, ch, c.redundantidx)

		// 6. collect metrics related to foreign key constraints with different data types.
		collectSchemaFKDatatypeMismatch(conn, ch, c.difftypefkey)

		// Function below uses queries pg_sequences which is introduced in Postgres 10.
		if config.ServerVersionNum < PostgresV10 {
			log.Debugln("[postgres schema collector]: some system views are not available, required Postgres 10 or newer")
			conn.Close()
			continue
		}

		// 7. collect metrics related to sequences (available since Postgres 10).
		collectSchemaSequences(conn, ch, c.sequences)

		conn.Close()
	}

	return nil
}

// collectSystemCatalogSize collects system catalog size metrics.
func collectSystemCatalogSize(conn *store.DB, ch chan<- prometheus.Metric, desc typedDesc) {
	datname := conn.Conn().Config().Database
	size, err := getSystemCatalogSize(conn)
	if err != nil {
		log.Errorf("get system catalog size of database %s failed: %s; skip", datname, err)
		return
	}

	if size > 0 {
		ch <- desc.newConstMetric(size, datname)
	}
}

// getSystemCatalogSize returns size of system catalog in bytes.
func getSystemCatalogSize(conn *store.DB) (float64, error) {
	var query = `SELECT sum(pg_total_relation_size(relname::regclass)) AS bytes FROM pg_stat_sys_tables WHERE schemaname = 'pg_catalog'`
	var size int64 = 0
	if err := conn.Conn().QueryRow(context.Background(), query).Scan(&size); err != nil {
		return 0, err
	}
	return float64(size), nil
}

// collectSchemaNonPKTables collects metrics related to non-PK tables.
func collectSchemaNonPKTables(conn *store.DB, ch chan<- prometheus.Metric, desc typedDesc) {
	datname := conn.Conn().Config().Database
	tables, err := getSchemaNonPKTables(conn)
	if err != nil {
		log.Errorf("collect non-pk tables in database %s failed: %s; skip", datname, err)
		return
	}

	for _, t := range tables {
		// tables are the slice of strings where each string is the table's FQN in following format: schemaname/relname
		parts := strings.Split(t, "/")
		if len(parts) != 2 {
			log.Warnf("incorrect table FQ name: %s; skip", t)
			continue
		}
		ch <- desc.newConstMetric(1, datname, parts[0], parts[1])
	}
}

// getSchemaNonPKTables searches tables with no PRIMARY or UNIQUE keys in the database and return its names.
func getSchemaNonPKTables(conn *store.DB) ([]string, error) {
	var query = "SELECT n.nspname AS schema, c.relname AS table " +
		"FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid " +
		"WHERE NOT EXISTS (SELECT 1 FROM pg_index i WHERE c.oid = i.indrelid AND (i.indisprimary OR i.indisunique)) " +
		"AND c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')"

	rows, err := conn.Conn().Query(context.Background(), query)
	if err != nil {
		return nil, err
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

	return tables, nil
}

// collectSchemaInvalidIndexes collects metrics related to invalid indexes.
func collectSchemaInvalidIndexes(conn *store.DB, ch chan<- prometheus.Metric, desc typedDesc) {
	database := conn.Conn().Config().Database
	stats, err := getSchemaInvalidIndexes(conn)
	if err != nil {
		log.Errorf("get invalid indexes stats of database %s failed: %s; skip", database, err)
		return
	}

	for k, s := range stats {
		var (
			schema = s.labels["schema"]
			table  = s.labels["table"]
			index  = s.labels["index"]
			value  = s.values["bytes"]
		)

		if schema == "" || table == "" || index == "" {
			log.Warnf("incomplete invalid index FQ name: %s; skip", k)
			continue
		}

		ch <- desc.newConstMetric(value, database, schema, table, index)
	}
}

// getSchemaInvalidIndexes searches invalid indexes in the database and return its names if such indexes have been found.
func getSchemaInvalidIndexes(conn *store.DB) (map[string]postgresGenericStat, error) {
	var query = "SELECT c1.relnamespace::regnamespace::text AS schema, c2.relname AS table, c1.relname AS index, " +
		"pg_relation_size(c1.relname::regclass) AS bytes " +
		"FROM pg_index i JOIN pg_class c1 ON i.indexrelid = c1.oid JOIN pg_class c2 ON i.indrelid = c2.oid WHERE NOT i.indisvalid"
	res, err := conn.Query(query)
	if err != nil {
		return nil, err
	}

	return parsePostgresGenericStats(res, []string{"schema", "table", "index"}), nil
}

// collectSchemaNonIndexedFK collects metrics related to non indexed foreign key constraints.
func collectSchemaNonIndexedFK(conn *store.DB, ch chan<- prometheus.Metric, desc typedDesc) {
	database := conn.Conn().Config().Database
	stats, err := getSchemaNonIndexedFK(conn)
	if err != nil {
		log.Errorf("get non-indexed fkeys stats of database %s failed: %s; skip", database, err)
		return
	}

	for k, s := range stats {
		var (
			schema     = s.labels["schema"]
			table      = s.labels["table"]
			columns    = s.labels["columns"]
			constraint = s.labels["constraint"]
			referenced = s.labels["referenced"]
		)

		if schema == "" || table == "" || columns == "" || constraint == "" || referenced == "" {
			log.Warnf("incomplete non-indexed foreign key constraint name: %s; skip", k)
			continue
		}

		ch <- desc.newConstMetric(1, database, schema, table, columns, constraint, referenced)
	}
}

// getSchemaNonIndexedFK searches non indexes foreign key constraints and return its names.
func getSchemaNonIndexedFK(conn *store.DB) (map[string]postgresGenericStat, error) {
	var query = "SELECT c.connamespace::regnamespace::text AS schema, s.relname AS table, " +
		"string_agg(a.attname, ',' ORDER BY x.n) AS columns, c.conname AS constraint, " +
		"c.confrelid::regclass::text AS referenced " +
		"FROM pg_constraint c CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS x(attnum, n) " +
		"JOIN pg_attribute a ON a.attnum = x.attnum AND a.attrelid = c.conrelid " +
		"JOIN pg_class s ON c.conrelid = s.oid " +
		"WHERE NOT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.conrelid AND (i.indkey::integer[])[0:cardinality(c.conkey)-1] @> c.conkey::integer[]) " +
		"AND c.contype = 'f' " +
		"GROUP BY c.connamespace,s.relname,c.conname,c.confrelid"

	res, err := conn.Query(query)
	if err != nil {
		return nil, err
	}

	return parsePostgresGenericStats(res, []string{"schema", "table", "columns", "constraint", "referenced"}), nil
}

// collectSchemaRedundantIndexes collects metrics related to invalid indexes
func collectSchemaRedundantIndexes(conn *store.DB, ch chan<- prometheus.Metric, desc typedDesc) {
	database := conn.Conn().Config().Database
	stats, err := getSchemaRedundantIndexes(conn)
	if err != nil {
		log.Errorf("get redundant indexes stats of database %s failed: %s; skip", database, err)
		return
	}

	for k, s := range stats {
		var (
			schema       = s.labels["schema"]
			table        = s.labels["table"]
			index        = s.labels["index"]
			indexdef     = s.labels["indexdef"]
			redundantdef = s.labels["redundantdef"]
			value        = s.values["bytes"]
		)

		if schema == "" || table == "" || index == "" || indexdef == "" || redundantdef == "" {
			log.Warnf("incomplete redundant index name: %s; skip", k)
			continue
		}

		ch <- desc.newConstMetric(value, database, schema, table, index, indexdef, redundantdef)
	}
}

// getSchemaRedundantIndexes searches redundant indexes and returns its sizes
func getSchemaRedundantIndexes(conn *store.DB) (map[string]postgresGenericStat, error) {
	var query = "WITH index_data AS (SELECT *, string_to_array(indkey::text,' ') AS key_array, array_length(string_to_array(indkey::text,' '),1) AS nkeys FROM pg_index) " +
		"SELECT c1.relnamespace::regnamespace::text AS schema, c1.relname AS table, c2.relname AS index, " +
		"pg_get_indexdef(i1.indexrelid) AS indexdef, pg_get_indexdef(i2.indexrelid) AS redundantdef, " +
		"pg_relation_size(i2.indexrelid) AS bytes " +
		"FROM index_data AS i1 JOIN index_data AS i2 ON i1.indrelid = i2.indrelid AND i1.indexrelid<>i2.indexrelid " +
		"JOIN pg_class c1 ON i1.indrelid = c1.oid " +
		"JOIN pg_class c2 ON i2.indexrelid = c2.oid " +
		`WHERE (regexp_replace(i1.indpred, 'location \\d+', 'location', 'g') IS NOT DISTINCT FROM regexp_replace(i2.indpred, 'location \\d+', 'location', 'g')) ` +
		`AND (regexp_replace(i1.indexprs, 'location \\d+', 'location', 'g') IS NOT DISTINCT FROM regexp_replace(i2.indexprs, 'location \\d+', 'location', 'g')) ` +
		"AND ((i1.nkeys > i2.nkeys AND NOT i2.indisunique) OR (i1.nkeys = i2.nkeys AND ((i1.indisunique AND i2.indisunique AND (i1.indexrelid>i2.indexrelid)) " +
		"OR (NOT i1.indisunique AND NOT i2.indisunique AND (i1.indexrelid>i2.indexrelid)) " +
		"OR (i1.indisunique AND NOT i2.indisunique)))) AND i1.key_array[1:i2.nkeys]=i2.key_array"

	res, err := conn.Query(query)
	if err != nil {
		return nil, err
	}

	return parsePostgresGenericStats(res, []string{"schema", "table", "index", "indexdef", "redundantdef"}), nil
}

// collectSchemaSequences collects metrics related to sequences attached to poor-typed columns.
func collectSchemaSequences(conn *store.DB, ch chan<- prometheus.Metric, desc typedDesc) {
	database := conn.Conn().Config().Database
	stats, err := getSchemaSequences(conn)
	if err != nil {
		log.Errorf("get sequences stats of database %s failed: %s; skip", database, err)
		return
	}

	for k, s := range stats {
		var (
			schema   = s.labels["schema"]
			sequence = s.labels["sequence"]
			value    = s.values["ratio"]
		)

		if schema == "" || sequence == "" {
			log.Warnf("incomplete sequence FQ name: %s; skip", k)
			continue
		}

		ch <- desc.newConstMetric(value, database, schema, sequence)
	}
}

// getSchemaSequences searches sequences attached to the poor-typed columns with risk of exhaustion.
func getSchemaSequences(conn *store.DB) (map[string]postgresGenericStat, error) {
	var query = `SELECT schemaname AS schema, sequencename AS sequence, coalesce(last_value, 0) / max_value::float AS ratio FROM pg_sequences`

	res, err := conn.Query(query)
	if err != nil {
		return nil, err
	}

	return parsePostgresGenericStats(res, []string{"schema", "sequence"}), nil
}

// collectSchemaFKDatatypeMismatch collects metrics related to foreign key constraints with different data types.
func collectSchemaFKDatatypeMismatch(conn *store.DB, ch chan<- prometheus.Metric, desc typedDesc) {
	database := conn.Conn().Config().Database
	stats, err := getSchemaFKDatatypeMismatch(conn)
	if err != nil {
		log.Errorf("get foreign keys data types stats of database %s failed: %s; skip", database, err)
		return
	}

	for k, s := range stats {
		var (
			schema    = s.labels["schema"]
			table     = s.labels["table"]
			column    = s.labels["column"]
			refschema = s.labels["refschema"]
			reftable  = s.labels["reftable"]
			refcolumn = s.labels["refcolumn"]
		)

		if schema == "" || table == "" || column == "" || refschema == "" || reftable == "" || refcolumn == "" {
			log.Warnf("incomplete FQ name %s in database %s; skip", k, database)
			continue
		}

		ch <- desc.newConstMetric(1, database, schema, table, column, refschema, reftable, refcolumn)
	}
}

// getSchemaFKDatatypeMismatch searches foreign key constraints with different data types.
func getSchemaFKDatatypeMismatch(conn *store.DB) (map[string]postgresGenericStat, error) {
	var query = "SELECT c1.relnamespace::regnamespace::text AS schema, c1.relname AS table, a1.attname||'::'||t1.typname AS column, " +
		"c2.relnamespace::regnamespace::text AS refschema, c2.relname AS reftable, a2.attname||'::'||t2.typname AS refcolumn " +
		"FROM pg_constraint JOIN pg_class c1 ON c1.oid = conrelid JOIN pg_class c2 ON c2.oid = confrelid " +
		"JOIN pg_attribute a1 ON a1.attnum = conkey[1] AND a1.attrelid = conrelid " +
		"JOIN pg_attribute a2 ON a2.attnum = confkey[1] AND a2.attrelid = confrelid " +
		"JOIN pg_type t1 ON t1.oid = a1.atttypid " +
		"JOIN pg_type t2 ON t2.oid = a2.atttypid " +
		"WHERE a1.atttypid <> a2.atttypid AND contype = 'f'"

	res, err := conn.Query(query)
	if err != nil {
		return nil, err
	}

	return parsePostgresGenericStats(res, []string{"schema", "table", "column", "refschema", "reftable", "refcolumn"}), nil
}
