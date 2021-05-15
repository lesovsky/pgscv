package collector

import (
	"database/sql"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"github.com/weaponry/pgscv/internal/model"
	"testing"
)

func TestPostgresWalArchivingCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{},
		optional: []string{
			"postgres_archiver_archived_total",
			"postgres_archiver_failed_total",
			"postgres_archiver_since_last_archive_seconds",
			"postgres_archiver_lag_bytes",
		},
		collector: NewPostgresWalArchivingCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

//func Test_parsePostgresWalArchivingStats(t *testing.T) {
//	var testCases = []struct {
//		name string
//		res  *model.PGResult
//		want postgresWalArchivingStat
//	}{
//		{
//			name: "normal output",
//			res: &model.PGResult{
//				Nrows: 1,
//				Ncols: 4,
//				Colnames: []pgproto3.FieldDescription{
//					{Name: []byte("archived_count")}, {Name: []byte("failed_count")},
//					{Name: []byte("since_last_archive_seconds")}, {Name: []byte("lag_bytes")},
//				},
//				Rows: [][]sql.NullString{
//					{
//						{String: "4587", Valid: true}, {String: "0", Valid: true},
//						{String: "17", Valid: true}, {String: "12345678", Valid: true},
//					},
//				},
//			},
//			want: postgresWalArchivingStat{archived: 4587, failed: 0, sinceArchivedSeconds: 17, lagBytes: 12345678},
//		},
//		{
//			name: "no rows output",
//			res: &model.PGResult{
//				Nrows: 0,
//				Ncols: 5,
//				Colnames: []pgproto3.FieldDescription{
//					{Name: []byte("archived_count")}, {Name: []byte("failed_count")},
//					{Name: []byte("since_last_archive_seconds")}, {Name: []byte("lag_bytes")},
//				},
//				Rows: [][]sql.NullString{},
//			},
//			want: postgresWalArchivingStat{archived: 0, failed: 0, sinceArchivedSeconds: 0, lagBytes: 0},
//		},
//	}
//
//	for _, tc := range testCases {
//		t.Run(tc.name, func(t *testing.T) {
//			got := parsePostgresWalArchivingStats(tc.res)
//			assert.EqualValues(t, tc.want, got)
//		})
//	}
//}
