package collector

import (
	"context"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPostgresSchemaCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_schema_system_catalog_bytes_total",
			"postgres_schema_non_pk_tables_total",
			"postgres_schema_invalid_index_bytes_total",
			"postgres_schema_non_indexed_fk_total",
			"postgres_schema_redundant_indexes_bytes_total",
			"postgres_schema_seq_exhaustion_ratio",
			"postgres_schema_mistyped_fkeys_total",
		},
		collector: NewPostgresSchemaCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_getSystemCatalogSize(t *testing.T) {
	conn := store.NewTest(t)
	assert.NotEqual(t, float64(0), getSystemCatalogSize(conn))

	_ = conn.Conn().Close(context.Background())
	assert.Equal(t, float64(0), getSystemCatalogSize(conn))
}

func Test_getSchemaNonPKTables(t *testing.T) {
	conn := store.NewTest(t)
	assert.Less(t, 0, len(getSchemaNonPKTables(conn)))

	_ = conn.Conn().Close(context.Background())
	assert.Equal(t, 0, len(getSchemaNonPKTables(conn)))
}

func Test_getSchemaInvalidIndexes(t *testing.T) {
	conn := store.NewTest(t)
	assert.Less(t, 0, len(getSchemaInvalidIndexes(conn)))

	_ = conn.Conn().Close(context.Background())
	assert.Equal(t, 0, len(getSchemaInvalidIndexes(conn)))
}

func Test_getSchemaNonIndexedFK(t *testing.T) {
	conn := store.NewTest(t)
	assert.Less(t, 0, len(getSchemaNonIndexedFK(conn)))

	_ = conn.Conn().Close(context.Background())
	assert.Equal(t, 0, len(getSchemaNonIndexedFK(conn)))
}

func Test_getSchemaRedundantIndexes(t *testing.T) {
	conn := store.NewTest(t)
	assert.Less(t, 0, len(getSchemaRedundantIndexes(conn)))

	_ = conn.Conn().Close(context.Background())
	assert.Equal(t, 0, len(getSchemaRedundantIndexes(conn)))
}

func Test_getSchemaSequences(t *testing.T) {
	conn := store.NewTest(t)
	assert.Less(t, 0, len(getSchemaSequences(conn)))

	_ = conn.Conn().Close(context.Background())
	assert.Equal(t, 0, len(getSchemaSequences(conn)))
}

func Test_getSchemaFKDatatypeMismatch(t *testing.T) {
	conn := store.NewTest(t)
	assert.Less(t, 0, len(getSchemaFKDatatypeMismatch(conn)))

	_ = conn.Conn().Close(context.Background())
	assert.Equal(t, 0, len(getSchemaFKDatatypeMismatch(conn)))
}
