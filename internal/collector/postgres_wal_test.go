package collector

import (
	"github.com/stretchr/testify/assert"
	"github.com/weaponry/pgscv/internal/model"
	"testing"
)

func TestPostgresWalCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_recovery_info",
			"postgres_wal_written_bytes_total",
		},
		optional:  []string{},
		collector: NewPostgresWalCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_selectWalQuery(t *testing.T) {
	var testcases = []struct {
		version int
		want    string
	}{
		{version: 90600, want: postgresWalQuery96},
		{version: 90605, want: postgresWalQuery96},
		{version: 100000, want: postgresWalQuertLatest},
		{version: 100005, want: postgresWalQuertLatest},
	}

	for _, tc := range testcases {
		t.Run("", func(t *testing.T) {
			assert.Equal(t, tc.want, selectWalQuery(tc.version))
		})
	}
}
