package stat

import (
	"database/sql"
	"fmt"
)

// Container for basic Postgres stats collected from pg_stat_* views
type PGresult struct {
	Result    [][]sql.NullString /* values */
	Cols      []string           /* list of columns' names*/
	Ncols     int                /* numbers of columns in Result */
	Nrows     int                /* number of rows in Result */
}

// Container for stats
// Read stat from pg_stat_* views and create PGresult struct
func (s *PGresult) GetPgstatSample(conn *sql.DB, query string) error {
	*s = PGresult{}
	rows, err := conn.Query(query)
	// Queries' errors aren't critical for us, remember and show them to the user. Return after the error, because
	// there is no reason to continue.
	if err != nil {
		return err
	}

	if err := s.New(rows); err != nil {
		return err
	}

	return nil
}

// Parse a result of the query and create PGresult struct
func (r *PGresult) New(rs *sql.Rows) error {
	var container []sql.NullString
	var pointers []interface{}

	r.Cols, _ = rs.Columns()
	r.Ncols = len(r.Cols)

	for rs.Next() {
		pointers = make([]interface{}, r.Ncols)
		container = make([]sql.NullString, r.Ncols)

		for i := range pointers {
			pointers[i] = &container[i]
		}

		err := rs.Scan(pointers...)
		if err != nil {
			return fmt.Errorf("Failed to scan row: %s", err)
		}

		// Yes, it's better to avoid append() here, but we can't pre-allocate array of required size due to there is no
		// simple way (built-in in db driver/package) to know how many rows are returned by query.
		r.Result = append(r.Result, container)
		r.Nrows++
	}

	// parsing successful
	return nil
}
