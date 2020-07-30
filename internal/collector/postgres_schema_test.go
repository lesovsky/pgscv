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

func Test_(t *testing.T) {
	conn := store.NewTest(t)
	// TODO: should be enabled only for integration tests with predefined fixtures
	//assert.Greater(t, 0, len(getSchemaNonPKTables(conn)))

	_ = conn.Conn().Close(context.Background())
	assert.Equal(t, 0, len(getSchemaNonPKTables(conn)))
}
