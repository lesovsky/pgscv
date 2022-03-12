package collector

import (
	"context"
	"github.com/lesovsky/pgscv/internal/model"
	"github.com/lesovsky/pgscv/internal/store"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPostgresSchemaCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_schema_system_catalog_bytes",
			"postgres_schema_non_pk_tables",
			"postgres_schema_invalid_indexes_bytes",
			"postgres_schema_non_indexed_fkeys",
			"postgres_schema_redundant_indexes_bytes",
			"postgres_schema_sequence_exhaustion_ratio",
			"postgres_schema_mistyped_fkeys",
		},
		collector: NewPostgresSchemasCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_getSystemCatalogSize(t *testing.T) {
	conn := store.NewTest(t)
	got, err := getSystemCatalogSize(conn)
	assert.NoError(t, err)
	assert.NotEqual(t, float64(0), got)

	_ = conn.Conn().Close(context.Background())
	got, err = getSystemCatalogSize(conn)
	assert.Error(t, err)
	assert.Equal(t, float64(0), got)
}

func Test_getSchemaNonPKTables(t *testing.T) {
	conn := store.NewTest(t)
	got, err := getSchemaNonPKTables(conn)
	assert.NoError(t, err)
	assert.Less(t, 0, len(got))

	_ = conn.Conn().Close(context.Background())
	got, err = getSchemaNonPKTables(conn)
	assert.Error(t, err)
	assert.Equal(t, 0, len(got))
}

func Test_getSchemaInvalidIndexes(t *testing.T) {
	conn := store.NewTest(t)
	got, err := getSchemaInvalidIndexes(conn)
	assert.NoError(t, err)
	assert.Less(t, 0, len(got))

	_ = conn.Conn().Close(context.Background())
	got, err = getSchemaInvalidIndexes(conn)
	assert.Error(t, err)
	assert.Equal(t, 0, len(got))
}

func Test_getSchemaNonIndexedFK(t *testing.T) {
	conn := store.NewTest(t)
	got, err := getSchemaNonIndexedFK(conn)
	assert.NoError(t, err)
	assert.Less(t, 0, len(got))

	_ = conn.Conn().Close(context.Background())
	got, err = getSchemaNonIndexedFK(conn)
	assert.Error(t, err)
	assert.Equal(t, 0, len(got))
}

func Test_getSchemaRedundantIndexes(t *testing.T) {
	conn := store.NewTest(t)
	got, err := getSchemaRedundantIndexes(conn)
	assert.NoError(t, err)
	assert.Less(t, 0, len(got))

	_ = conn.Conn().Close(context.Background())
	got, err = getSchemaRedundantIndexes(conn)
	assert.Error(t, err)
	assert.Equal(t, 0, len(got))
}

func Test_getSchemaSequences(t *testing.T) {
	conn := store.NewTest(t)
	got, err := getSchemaSequences(conn)
	assert.NoError(t, err)
	assert.Less(t, 0, len(got))

	_ = conn.Conn().Close(context.Background())
	got, err = getSchemaSequences(conn)
	assert.Error(t, err)
	assert.Equal(t, 0, len(got))
}

func Test_getSchemaFKDatatypeMismatch(t *testing.T) {
	conn := store.NewTest(t)
	got, err := getSchemaFKDatatypeMismatch(conn)
	assert.NoError(t, err)
	assert.Less(t, 0, len(got))

	_ = conn.Conn().Close(context.Background())
	got, err = getSchemaFKDatatypeMismatch(conn)
	assert.Error(t, err)
	assert.Equal(t, 0, len(got))
}
