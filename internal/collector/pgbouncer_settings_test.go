package collector

import (
	"database/sql"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPgbouncerSettingsCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"pgbouncer_service_settings_info",
			"pgbouncer_service_database_settings_info",
			"pgbouncer_service_database_pool_size",
		},
		collector: NewPgbouncerSettingsCollector,
		service:   model.ServiceTypePgbouncer,
	}

	pipeline(t, input)
}

func Test_parsePgbouncerSettings(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want map[string]string
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 2,
				Ncols: 3,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("key")}, {Name: []byte("value")}, {Name: []byte("changeable")},
				},
				Rows: [][]sql.NullString{
					{{String: "listen_addr", Valid: true}, {String: "127.0.0.1", Valid: true}, {String: "no", Valid: true}},
					{{String: "max_client_conn", Valid: true}, {String: "1000", Valid: true}, {String: "yes", Valid: true}},
				},
			},
			want: map[string]string{
				"listen_addr":     "127.0.0.1",
				"max_client_conn": "1000",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePgbouncerSettings(tc.res)
			assert.EqualValues(t, tc.want, got)
		})
	}
}

func Test_getPerDatabaseSettings(t *testing.T) {
	defaults := map[string]string{
		"pool_mode":         "transaction",
		"default_pool_size": "35",
	}

	want := []dbSettings{
		{name: "test1", mode: "transaction", size: "30"},
		{name: "test2", mode: "transaction", size: "30"},
		{name: "test3", mode: "transaction", size: "20"},
		{name: "test4", mode: "session", size: "10"},
		{name: "*", mode: "transaction", size: "10"},
	}

	got, err := getPerDatabaseSettings("./testdata/pgbouncer/pgbouncer.ini.golden", defaults)
	assert.NoError(t, err)
	assert.Equal(t, want, got)

	_, err = getPerDatabaseSettings("./testdata/pgbouncer/unknown.file", defaults)
	assert.Error(t, err)
}

func Test_parsePoolSettingsLine(t *testing.T) {
	testcases := []struct {
		in    string
		want  dbSettings
		valid bool
	}{
		{in: "test = host=1.2.3.4 pool_size=50", want: dbSettings{name: "test", mode: "", size: "50"}, valid: true},
		{in: "test = host=1.2.3.4 pool_size=50 pool_mode=transaction", want: dbSettings{name: "test", mode: "transaction", size: "50"}, valid: true},
		{in: "test =", want: dbSettings{name: "test", mode: "", size: ""}, valid: true},
		{in: "invalid", valid: false},
	}

	for _, tc := range testcases {
		got, err := parseDatabaseSettingsLine(tc.in)
		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		} else {
			assert.Error(t, err)
		}
	}
}
