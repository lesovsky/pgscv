package collector

import (
	"database/sql"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"testing"
)

func Test_parsePostgresGenericStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want map[string]postgresGenericStat
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 2,
				Ncols: 6,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("label1")}, {Name: []byte("label2")},
					{Name: []byte("value1")}, {Name: []byte("value2")}, {Name: []byte("value3")}, {Name: []byte("value4")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "name1", Valid: true}, {String: "name2", Valid: true},
						{String: "1", Valid: true}, {String: "2", Valid: true}, {String: "3", Valid: true}, {String: "4", Valid: true},
					},
					{
						{String: "name3", Valid: true}, {String: "name4", Valid: true},
						{String: "5", Valid: true}, {String: "6", Valid: true}, {String: "7", Valid: true}, {String: "8", Valid: true},
					},
				},
			},
			want: map[string]postgresGenericStat{
				"name1/name2": {
					labels: map[string]string{"label1": "name1", "label2": "name2"},
					values: map[string]float64{"value1": 1, "value2": 2, "value3": 3, "value4": 4},
				},
				"name3/name4": {
					labels: map[string]string{"label1": "name3", "label2": "name4"},
					values: map[string]float64{"value1": 5, "value2": 6, "value3": 7, "value4": 8},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresGenericStats(tc.res, []string{"label1", "label2"})
			assert.EqualValues(t, tc.want, got)
		})
	}
}

func Test_parsePostgresCustomStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want postgresCustomStat
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 3,
				Ncols: 6,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("label1")}, {Name: []byte("label2")},
					{Name: []byte("value1")}, {Name: []byte("value2")}, {Name: []byte("value3")}, {Name: []byte("value4")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "name1", Valid: true}, {String: "name2", Valid: true},
						{String: "1", Valid: true}, {String: "2", Valid: true}, {String: "3", Valid: true}, {String: "4", Valid: true},
					},
					{
						{String: "name3", Valid: true}, {String: "name4", Valid: true},
						{String: "5", Valid: true}, {String: "6", Valid: true}, {String: "7", Valid: true}, {String: "8", Valid: true},
					},
					{
						{String: "name5", Valid: true}, {String: "name6", Valid: true},
						{String: "5", Valid: true}, {String: "6", Valid: true}, {String: "7", Valid: true}, {String: "", Valid: false},
					},
				},
			},
			want: postgresCustomStat{
				"name1/name2": customValues{"value1": 1, "value2": 2, "value3": 3, "value4": 4},
				"name3/name4": customValues{"value1": 5, "value2": 6, "value3": 7, "value4": 8},
				"name5/name6": customValues{"value1": 5, "value2": 6, "value3": 7},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresCustomStats(tc.res, []string{"label1", "label2"})
			assert.EqualValues(t, tc.want, got)
		})
	}
}

func Test_listDatabases(t *testing.T) {
	conn := store.NewTest(t)

	databases, err := listDatabases(conn)
	assert.NoError(t, err)
	assert.Greater(t, len(databases), 0)
	conn.Close()
}

func Test_isExtensionAvailable(t *testing.T) {
	conn := store.NewTest(t)

	assert.True(t, isExtensionAvailable(conn, "plpgsql"))
	assert.False(t, isExtensionAvailable(conn, "invalid"))
	conn.Close()
}
