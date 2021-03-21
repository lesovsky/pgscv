package collector

import (
	"database/sql"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"os"
	"testing"
)

func TestPostgresStorageCollector_Update(t *testing.T) {
	if uid := os.Geteuid(); uid != 0 {
		t.Skipf("root privileges required, skip")
	}

	var input = pipelineInput{
		required: []string{
			"postgres_data_directory_bytes",
			"postgres_wal_directory_bytes", "postgres_wal_directory_files",
			"postgres_log_directory_bytes", "postgres_log_directory_files",
			"postgres_temp_files_all_bytes",
		},
		// temp files related metrics might not be generated on idle systems.
		optional: []string{
			"postgres_temp_files_in_flight",
			"postgres_temp_bytes_in_flight",
			"postgres_temp_files_max_age_seconds",
		},
		collector: NewPostgresStorageCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresTempFileInflght(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want map[string]postgresTempfilesStat
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 4,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("tablespace")}, {Name: []byte("files_total")}, {Name: []byte("bytes_total")}, {Name: []byte("max_age_seconds")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testtablespace", Valid: true}, {String: "45", Valid: true}, {String: "84523654741", Valid: true}, {String: "578", Valid: true},
					},
				},
			},
			want: map[string]postgresTempfilesStat{
				"testtablespace": {tablespace: "testtablespace", tempfiles: 45, tempbytes: 84523654741, tempmaxage: 578},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresTempFileInflght(tc.res)
			assert.EqualValues(t, tc.want, got)
		})
	}
}

func Test_getDatadirStat(t *testing.T) {
	if uid := os.Geteuid(); uid != 0 {
		t.Skipf("root privileges required, skip")
	}

	mounts, err := getMountpoints()
	assert.NoError(t, err)

	s1, s2, i1, err := getDatadirStat("/tmp", mounts)
	assert.NoError(t, err)
	assert.NotEqual(t, "", s1)
	assert.NotEqual(t, "", s2)
	assert.NotEqual(t, 0, i1)
}

func Test_getWaldirStat(t *testing.T) {
	if uid := os.Geteuid(); uid != 0 {
		t.Skipf("root privileges required, skip")
	}

	mounts, err := getMountpoints()
	assert.NoError(t, err)

	conn := store.NewTest(t)

	s1, s2, s3, i1, i2, err := getWaldirStat(conn, mounts)
	assert.NoError(t, err)
	assert.NotEqual(t, "", s1)
	assert.NotEqual(t, "", s2)
	assert.NotEqual(t, "", s3)
	assert.NotEqual(t, 0, i1)
	assert.NotEqual(t, 0, i2)

	conn.Close()
}

func Test_getLogdirStat(t *testing.T) {
	mounts, err := getMountpoints()
	assert.NoError(t, err)

	conn := store.NewTest(t)

	s1, s2, s3, i1, i2, err := getLogdirStat(conn, true, "/tmp", mounts)
	assert.NoError(t, err)
	assert.NotEqual(t, "", s1)
	assert.NotEqual(t, "", s2)
	assert.NotEqual(t, "", s3)
	assert.NotEqual(t, 0, i1)
	assert.NotEqual(t, 0, i2)

	conn.Close()
}

func Test_getTempfilesStat(t *testing.T) {
	conn := store.NewTest(t)

	_, _, err := getTempfilesStat(conn, 120000)
	assert.NoError(t, err)

	conn.Close()
}

func Test_getDirectorySize(t *testing.T) {
	size, err := getDirectorySize("testdata")
	assert.NoError(t, err)
	assert.Greater(t, size, int64(0))

	size, err = getDirectorySize("unknown")
	assert.Error(t, err)
	assert.Equal(t, size, int64(0))
}

func Test_findMountpoint(t *testing.T) {
	mount, device, err := findMountpoint([]mount{{mountpoint: "/", device: "sda"}}, "/bin")
	assert.NoError(t, err)
	assert.Equal(t, "/", mount)
	assert.Equal(t, "sda", device)
}

func Test_getMountpoints(t *testing.T) {
	res, err := getMountpoints()
	assert.NoError(t, err)
	assert.Greater(t, len(res), 0)
}
