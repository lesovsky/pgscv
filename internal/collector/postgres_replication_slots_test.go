package collector

import (
	"database/sql"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPostgresReplicationSlotCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{},
		optional: []string{
			"postgres_replication_slot_wal_retain_bytes",
		},
		collector: NewPostgresReplicationSlotCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresReplicationSlotStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want map[string]postgresReplicationSlotStat
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 15,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("slot_name")}, {Name: []byte("slot_type")}, {Name: []byte("database")}, {Name: []byte("active")}, {Name: []byte("since_restart_bytes")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testslot", Valid: true}, {String: "testtype", Valid: true}, {String: "testdb", Valid: true}, {String: "t", Valid: true}, {String: "25485425", Valid: true},
					},
				},
			},
			want: map[string]postgresReplicationSlotStat{
				"testdb/testslot/testtype": {slotname: "testslot", slottype: "testtype", database: "testdb", active: "t", retainedBytes: 25485425},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresReplicationSlotStats(tc.res, []string{"slot_name", "slot_type", "database", "active"})
			assert.EqualValues(t, tc.want, got)
		})
	}
}

func Test_selectReplicationSlotQuery(t *testing.T) {
	var testcases = []struct {
		version int
		want    string
	}{
		{version: 90600, want: postgresReplicationSlotQuery96},
		{version: 90605, want: postgresReplicationSlotQuery96},
		{version: 100000, want: postgresReplicationSlotQueryLatest},
		{version: 100005, want: postgresReplicationSlotQueryLatest},
	}

	for _, tc := range testcases {
		t.Run("", func(t *testing.T) {
			assert.Equal(t, tc.want, selectReplicationSlotQuery(tc.version))
		})
	}
}
