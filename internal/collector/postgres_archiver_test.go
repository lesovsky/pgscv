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

func Test_parsePostgresWalArchivingStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want postgresWalArchivingStat
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 5,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("archived_count")}, {Name: []byte("failed_count")},
					{Name: []byte("since_last_archive_seconds")},
					{Name: []byte("last_modified_wal")}, {Name: []byte("last_archived_wal")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "4587", Valid: true}, {String: "0", Valid: true},
						{String: "17", Valid: true},
						{String: "000000010000002A000000FE", Valid: true}, {String: "000000010000002A000000FD", Valid: true},
					},
				},
			},
			want: postgresWalArchivingStat{
				archived: 4587, failed: 0, sinceArchivedSeconds: 17, segLastModified: "000000010000002A000000FE", segLastArchived: "000000010000002A000000FD",
			},
		},
		{
			name: "no rows output",
			res: &model.PGResult{
				Nrows: 0,
				Ncols: 5,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("archived_count")}, {Name: []byte("failed_count")},
					{Name: []byte("since_last_archive_seconds")},
					{Name: []byte("last_modified_wal")}, {Name: []byte("last_archived_wal")},
				},
				Rows: [][]sql.NullString{},
			},
			want: postgresWalArchivingStat{
				archived: 0, failed: 0, sinceArchivedSeconds: 0, segLastModified: "", segLastArchived: "",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresWalArchivingStats(tc.res)
			assert.EqualValues(t, tc.want, got)
		})
	}
}

func Test_countWalArchivingLag(t *testing.T) {
	testcases := []struct {
		valid        bool
		lastModified string
		lastArchived string
		segsz        uint64
		want         float64
	}{
		{valid: true, lastModified: "0000000100000052000000E9", lastArchived: "0000000100000052000000E9", segsz: 16 * 1024 * 1024, want: 0},
		{valid: true, lastModified: "0000000100000052000000E9", lastArchived: "0000000100000052000000E8", segsz: 16 * 1024 * 1024, want: 0},
		{valid: true, lastModified: "0000000100000052000000E9", lastArchived: "0000000100000052000000E7", segsz: 16 * 1024 * 1024, want: 16777216},
		{valid: true, lastModified: "0000000100000052000000E9", lastArchived: "000000010000005200000042", segsz: 16 * 1024 * 1024, want: 2785017856},
		{valid: true, lastModified: "000000010000003200000029", lastArchived: "000000010000003200000028", segsz: 64 * 1024 * 1024, want: 0},
		{valid: true, lastModified: "000000010000003200000029", lastArchived: "000000010000003200000027", segsz: 64 * 1024 * 1024, want: 67108864},
		{valid: true, lastModified: "000000010000003200000029", lastArchived: "000000010000003100000011", segsz: 64 * 1024 * 1024, want: 5838471168},
		{valid: false, lastModified: "0000000100000052000000E8", lastArchived: "0000000100000052000000E9", segsz: 16 * 1024 * 1024, want: 0},
		{valid: false, lastModified: "", lastArchived: "000000010000003100000011"},
		{valid: false, lastModified: "000000010000003100000011", lastArchived: "", segsz: 16 * 1024 * 1024},
	}

	for _, tc := range testcases {
		got, err := countWalArchivingLag(tc.lastModified, tc.lastArchived, tc.segsz)

		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
			//fmt.Println(got)
		} else {
			assert.Error(t, err)
		}
	}
}

func Test_parseWalFileName(t *testing.T) {
	testcases := []struct {
		valid bool
		name  string
		segsz uint64
		want  uint64
	}{
		{valid: true, name: "000000010000004C0000007A", segsz: 16 * 1024 * 1024, want: 19578},
		{valid: true, name: "000000010000004B0000007F", segsz: 32 * 1024 * 1024, want: 9727},
		{valid: true, name: "000000010000004C0000001E", segsz: 64 * 1024 * 1024, want: 4894},
		{valid: false, name: "000000010000004C0000001E", segsz: 1024 * 1023},        // invalid walSegSize
		{valid: false, name: "000000010000004C0000001E", segsz: 1024 * 1024 * 1025}, // invalid walSegSize
		{valid: false, name: "000000010000004C000007", segsz: 16 * 1024 * 1024},     // invalid segment name
		{valid: false, name: "000000010000004g0000007A", segsz: 16 * 1024 * 1024},   // invalid segment name
		{valid: false, name: "000000010000004C0000007g", segsz: 16 * 1024 * 1024},   // invalid segment name
		{valid: false, name: "000000010000004C000007A2", segsz: 16 * 1024 * 1024},   // invalid low number for 16MB segsz
		{valid: false, name: "000000010000004C0000007A", segsz: 64 * 1024 * 1024},   // invalid low number for 64MB segsz
	}

	for _, tc := range testcases {
		got, err := parseWalFileName(tc.name, tc.segsz)

		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		} else {
			assert.Error(t, err)
		}
	}
}
