package collector

import (
	"database/sql"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestPostgresSettingsCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_service_settings_info",
			"postgres_service_files_info",
		},
		collector: NewPostgresSettingsCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresSettings(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want []postgresSetting
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 4,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("name")}, {Name: []byte("setting")}, {Name: []byte("unit")}, {Name: []byte("vartype")},
				},
				Rows: [][]sql.NullString{
					{{String: "bgwriter_flush_after", Valid: true}, {String: "64", Valid: true}, {String: "8kB", Valid: true}, {String: "integer", Valid: true}},
					{{String: "max_connections", Valid: true}, {String: "100", Valid: true}, {String: "", Valid: true}, {String: "integer", Valid: true}},
				},
			},
			want: []postgresSetting{
				{name: "bgwriter_flush_after", setting: "524288", unit: "bytes", vartype: "integer", value: 524288},
				{name: "max_connections", setting: "100", unit: "", vartype: "integer", value: 100},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresSettings(tc.res)
			assert.EqualValues(t, tc.want, got)
		})
	}
}

func Test_parsePostgresFiles(t *testing.T) {
	// set exact permissions because after CI's git clone permissions depend on used system umask.
	assert.NoError(t, os.Chmod("testdata/datadir/postgresql.conf.golden", 0644))
	assert.NoError(t, os.Chmod("testdata/datadir/pg_hba.conf.golden", 0644))
	assert.NoError(t, os.Chmod("testdata/datadir/pg_ident.conf.golden", 0644))
	assert.NoError(t, os.Chmod("testdata/datadir", 0755))

	var testCases = []struct {
		name string
		res  *model.PGResult
		want []postgresFile
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows:    4,
				Ncols:    2,
				Colnames: []pgproto3.FieldDescription{{Name: []byte("name")}, {Name: []byte("setting")}},
				Rows: [][]sql.NullString{
					{{String: "config_file", Valid: true}, {String: "testdata/datadir/postgresql.conf.golden", Valid: true}},
					{{String: "hba_file", Valid: true}, {String: "testdata/datadir/pg_hba.conf.golden", Valid: true}},
					{{String: "ident_file", Valid: true}, {String: "testdata/datadir/pg_ident.conf.golden", Valid: true}},
					{{String: "data_directory", Valid: true}, {String: "testdata/datadir", Valid: true}},
				},
			},
			want: []postgresFile{
				{path: "testdata/datadir/postgresql.conf.golden", mode: "0644", guc: "config_file"},
				{path: "testdata/datadir/pg_hba.conf.golden", mode: "0644", guc: "hba_file"},
				{path: "testdata/datadir/pg_ident.conf.golden", mode: "0644", guc: "ident_file"},
				{path: "testdata/datadir", mode: "0755", guc: "data_directory"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresFiles(tc.res)
			assert.EqualValues(t, tc.want, got)
		})
	}
}

func Test_newPostgresSetting(t *testing.T) {
	var testCases = []struct {
		name    string
		setting string
		unit    string
		vartype string
		want    postgresSetting
		valid   bool
	}{
		// vartype 'enum'
		{
			valid: true, name: "archive_mode", setting: "off", unit: "", vartype: "enum",
			want: postgresSetting{name: "archive_mode", setting: "off", unit: "", vartype: "enum", value: 0},
		},
		{
			valid: true, name: "archive_mode", setting: "on", unit: "", vartype: "enum",
			want: postgresSetting{name: "archive_mode", setting: "on", unit: "", vartype: "enum", value: 0},
		},
		{
			valid: true, name: "archive_mode", setting: "always", unit: "", vartype: "enum",
			want: postgresSetting{name: "archive_mode", setting: "always", unit: "", vartype: "enum", value: 0},
		},
		{
			valid: true, name: "ssl_max_protocol_version", setting: "", unit: "", vartype: "enum",
			want: postgresSetting{name: "ssl_max_protocol_version", setting: "", unit: "", vartype: "enum", value: 0},
		},
		// vartype 'string'
		{
			valid: true, name: "archive_cleanup_command", setting: "", unit: "", vartype: "string",
			want: postgresSetting{name: "archive_cleanup_command", setting: "", unit: "", vartype: "string", value: 0},
		},
		{
			valid: true, name: "cluster_name", setting: "12/main", unit: "", vartype: "string",
			want: postgresSetting{name: "cluster_name", setting: "12/main", unit: "", vartype: "string", value: 0},
		},
		{
			valid: true, name: "log_line_prefix", setting: "%m %p %u@%d from %h [vxid:%v txid:%x] [%i] ", unit: "", vartype: "string",
			want: postgresSetting{name: "log_line_prefix", setting: "%m %p %u@%d from %h [vxid:%v txid:%x] [%i] ", unit: "", vartype: "string", value: 0},
		},
		// vartype 'bool'
		{
			valid: true, name: "allow_system_table_mods", setting: "off", unit: "", vartype: "bool",
			want: postgresSetting{name: "allow_system_table_mods", setting: "off", unit: "", vartype: "bool", value: 0},
		},
		{
			valid: true, name: "autovacuum", setting: "on", unit: "", vartype: "bool",
			want: postgresSetting{name: "autovacuum", setting: "on", unit: "", vartype: "bool", value: 1},
		},
		// vartype 'integer'
		{
			valid: true, name: "autovacuum_vacuum_cost_limit", setting: "-1", unit: "", vartype: "integer",
			want: postgresSetting{name: "autovacuum_vacuum_cost_limit", setting: "-1", unit: "", vartype: "integer", value: -1},
		},
		{
			valid: true, name: "autovacuum_vacuum_threshold", setting: "50", unit: "", vartype: "integer",
			want: postgresSetting{name: "autovacuum_vacuum_threshold", setting: "50", unit: "", vartype: "integer", value: 50},
		},
		{
			valid: true, name: "log_temp_files", setting: "0", unit: "kB", vartype: "integer",
			want: postgresSetting{name: "log_temp_files", setting: "0", unit: "bytes", vartype: "integer", value: 0},
		},
		{
			valid: true, name: "maintenance_work_mem", setting: "65536", unit: "kB", vartype: "integer",
			want: postgresSetting{name: "maintenance_work_mem", setting: "67108864", unit: "bytes", vartype: "integer", value: 67108864},
		},
		{
			valid: true, name: "bgwriter_flush_after", setting: "64", unit: "8kB", vartype: "integer",
			want: postgresSetting{name: "bgwriter_flush_after", setting: "524288", unit: "bytes", vartype: "integer", value: 524288},
		},
		{
			valid: true, name: "old_snapshot_threshold", setting: "-1", unit: "min", vartype: "integer",
			want: postgresSetting{name: "old_snapshot_threshold", setting: "-1", unit: "seconds", vartype: "integer", value: -1},
		},
		{
			valid: true, name: "bgwriter_delay", setting: "200", unit: "ms", vartype: "integer",
			want: postgresSetting{name: "bgwriter_delay", setting: "0.2", unit: "seconds", vartype: "integer", value: 0.2},
		},
		{
			valid: true, name: "archive_timeout", setting: "0", unit: "s", vartype: "integer",
			want: postgresSetting{name: "archive_timeout", setting: "0", unit: "seconds", vartype: "integer", value: 0},
		},
		{
			valid: true, name: "archive_timeout", setting: "60", unit: "s", vartype: "integer",
			want: postgresSetting{name: "archive_timeout", setting: "60", unit: "seconds", vartype: "integer", value: 60},
		},
		// vartype 'real'
		{
			valid: true, name: "cpu_operator_cost", setting: "0.0025", unit: "", vartype: "real",
			want: postgresSetting{name: "cpu_operator_cost", setting: "0.0025", unit: "", vartype: "real", value: 0.0025},
		},
		{
			valid: true, name: "autovacuum_analyze_scale_factor", setting: "0.01", unit: "", vartype: "real",
			want: postgresSetting{name: "autovacuum_analyze_scale_factor", setting: "0.01", unit: "", vartype: "real", value: 0.01},
		},
		{
			valid: true, name: "geqo_seed", setting: "0", unit: "", vartype: "real",
			want: postgresSetting{name: "geqo_seed", setting: "0", unit: "", vartype: "real", value: 0},
		},
		{
			valid: true, name: "geqo_seed", setting: "2", unit: "", vartype: "real",
			want: postgresSetting{name: "geqo_seed", setting: "2", unit: "", vartype: "real", value: 2},
		},
		{
			valid: true, name: "jit_above_cost", setting: "100000", unit: "", vartype: "real",
			want: postgresSetting{name: "jit_above_cost", setting: "100000", unit: "", vartype: "real", value: 100000},
		},
		{
			valid: true, name: "vacuum_cost_delay", setting: "0", unit: "ms", vartype: "real",
			want: postgresSetting{name: "vacuum_cost_delay", setting: "0", unit: "seconds", vartype: "real", value: 0},
		},
		{
			valid: true, name: "autovacuum_vacuum_cost_delay", setting: "2", unit: "ms", vartype: "real",
			want: postgresSetting{name: "autovacuum_vacuum_cost_delay", setting: "0.002", unit: "seconds", vartype: "real", value: 0.002},
		},
		// wrong cases
		{
			valid: false, name: "invalid_vartype", setting: "", unit: "", vartype: "unknown",
			want: postgresSetting{name: "invalid_vartype", setting: "", unit: "", vartype: "", value: 0},
		},
		{
			valid: false, name: "invalid_bool", setting: "invalid", unit: "", vartype: "bool",
			want: postgresSetting{name: "invalid_bool", setting: "", unit: "", vartype: "", value: 0},
		},
		{
			valid: false, name: "invalid_unit", setting: "", unit: "invalid", vartype: "integer",
			want: postgresSetting{name: "invalid_unit", setting: "", unit: "", vartype: "", value: 0},
		},
		{
			valid: false, name: "invalid_unit", setting: "", unit: "invalid", vartype: "real",
			want: postgresSetting{name: "invalid_unit", setting: "", unit: "", vartype: "", value: 0},
		},
		{
			valid: false, name: "invalid_value", setting: "invalid", unit: "kB", vartype: "integer",
			want: postgresSetting{name: "invalid_value", setting: "", unit: "", vartype: "", value: 0},
		},
		{
			valid: false, name: "invalid_value", setting: "invalid", unit: "kB", vartype: "real",
			want: postgresSetting{name: "invalid_value", setting: "", unit: "", vartype: "", value: 0},
		},
	}

	for _, tc := range testCases {
		got, err := newPostgresSetting(tc.name, tc.setting, tc.unit, tc.vartype)
		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		} else {
			assert.Error(t, err)
		}
	}
}

func Test_parseUnit(t *testing.T) {
	var testCases = []struct {
		unit       string
		wantUnit   string
		wantFactor float64
	}{
		{unit: "", wantUnit: "", wantFactor: 1},
		{unit: "B", wantUnit: "bytes", wantFactor: 1},
		{unit: "200B", wantUnit: "bytes", wantFactor: 200},
		{unit: "1100B", wantUnit: "bytes", wantFactor: 1100},
		{unit: "kB", wantUnit: "bytes", wantFactor: 1024},
		{unit: "8kB", wantUnit: "bytes", wantFactor: 8 * 1024},
		{unit: "55kB", wantUnit: "bytes", wantFactor: 55 * 1024},
		{unit: "485kB", wantUnit: "bytes", wantFactor: 485 * 1024},
		{unit: "MB", wantUnit: "bytes", wantFactor: 1024 * 1024},
		{unit: "1MB", wantUnit: "bytes", wantFactor: 1024 * 1024},
		{unit: "9MB", wantUnit: "bytes", wantFactor: 9 * 1024 * 1024},
		{unit: "16MB", wantUnit: "bytes", wantFactor: 16 * 1024 * 1024},
		{unit: "101MB", wantUnit: "bytes", wantFactor: 101 * 1024 * 1024},
		{unit: "GB", wantUnit: "bytes", wantFactor: 1024 * 1024 * 1024},
		{unit: "1GB", wantUnit: "bytes", wantFactor: 1024 * 1024 * 1024},
		{unit: "9GB", wantUnit: "bytes", wantFactor: 9 * 1024 * 1024 * 1024},
		{unit: "98GB", wantUnit: "bytes", wantFactor: 98 * 1024 * 1024 * 1024},
		{unit: "TB", wantUnit: "bytes", wantFactor: 1024 * 1024 * 1024 * 1024},
		{unit: "ms", wantUnit: "seconds", wantFactor: .001},
		{unit: "5ms", wantUnit: "seconds", wantFactor: .005},
		{unit: "84ms", wantUnit: "seconds", wantFactor: .084},
		{unit: "200ms", wantUnit: "seconds", wantFactor: .2},
		{unit: "642ms", wantUnit: "seconds", wantFactor: .642},
		{unit: "s", wantUnit: "seconds", wantFactor: 1},
		{unit: "5s", wantUnit: "seconds", wantFactor: 5},
		{unit: "48s", wantUnit: "seconds", wantFactor: 48},
		{unit: "384s", wantUnit: "seconds", wantFactor: 384},
		{unit: "min", wantUnit: "seconds", wantFactor: 60},
		{unit: "7min", wantUnit: "seconds", wantFactor: 7 * 60},
		{unit: "30min", wantUnit: "seconds", wantFactor: 30 * 60},
		{unit: "145min", wantUnit: "seconds", wantFactor: 145 * 60},
		{unit: "h", wantUnit: "seconds", wantFactor: 60 * 60},
		{unit: "2h", wantUnit: "seconds", wantFactor: 2 * 60 * 60},
		{unit: "15h", wantUnit: "seconds", wantFactor: 15 * 60 * 60},
		{unit: "d", wantUnit: "seconds", wantFactor: 60 * 60 * 24},
		{unit: "4d", wantUnit: "seconds", wantFactor: 4 * 60 * 60 * 24},
	}

	for _, tc := range testCases {
		factor, unit, err := parseUnit(tc.unit)
		assert.NoError(t, err)
		assert.Equal(t, tc.wantUnit, unit)
		assert.Equal(t, tc.wantFactor, factor)
	}

	_, _, err := parseUnit("invalid")
	assert.Error(t, err)

	_, _, err = parseUnit("8k8k")
	assert.Error(t, err)
}
