package collector

import (
	"context"
	"database/sql"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/jackc/pgproto3/v2"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
)

type typedDesc struct {
	// name is the name of column in a query output used for getting value for metric.
	colname string
	// desc is the descriptor used by every Prometheus Metric.
	desc *prometheus.Desc
	// valueType is an enumeration of metric types that represent a simple value.
	valueType prometheus.ValueType
}

// queryResult is the iterable store that contains result of query - data (values) and metadata (number of rows, columns and names).
type queryResult struct {
	nrows    int
	ncols    int
	colnames []pgproto3.FieldDescription
	rows     [][]sql.NullString
}

func getStats(db *store.DB, query string) (*queryResult, error) {
	rows, err := db.Conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}

	// Generic variables describe properties of query result.
	var (
		colnames = rows.FieldDescriptions()
		ncols    = len(colnames)
		nrows    int
	)

	// Storage variables used below for data extraction.
	// Scan operation supports only slice of interfaces, 'pointers' slice is the intermediate store where all values written.
	// Next values from 'pointers' associated with type-strict slice - 'values'. When Scan is writing to the 'pointers' it
	// also writing to the 'values' under the hood. When all pointers/values have been scanned, put them into 'rowsStore'.
	// Finally we get queryResult iterable store with data and information about stored rows, columns and columns names.
	var (
		//pointers  []interface{}
		//values    []sql.NullString
		rowsStore = make([][]sql.NullString, 0, 10)
	)

	for rows.Next() {
		pointers := make([]interface{}, ncols)
		values := make([]sql.NullString, ncols)

		for i := range pointers {
			pointers[i] = &values[i]
		}

		err = rows.Scan(pointers...)
		if err != nil {
			log.Warnf("skip collecting database stats: %s", err)
			continue // если произошла ошибка, то пропускаем эту строку целиком и переходим к следующей
		}
		rowsStore = append(rowsStore, values)
		nrows++
	}

	rows.Close()

	return &queryResult{
		nrows:    nrows,
		ncols:    ncols,
		colnames: colnames,
		rows:     rowsStore,
	}, nil
}

func parseStats(r *queryResult, ch chan<- prometheus.Metric, descs []typedDesc, labelNames []string) error {
	for _, row := range r.rows {
		for i, colname := range r.colnames {
			// Если колонки нет в списке меток, то генерим метрику на основе значения [row][column].
			// Если имя колонки входит в список меток, то пропускаем ее -- нам не нужно генерить из нее метрику, т.к. она как метка+значение сама будет частью метрики
			if !stringsContains(labelNames, string(colname.Name)) {
				var labelValues = make([]string, len(labelNames))

				// итерируемся по именам меток, нужно собрать из результата-ответа от базы, значения для соотв. меток
				for j, lname := range labelNames {
					// определяем номер (индекс) колонки в PGresult, который соотв. названию метки -- по этому индексу возьмем значение для метки из PGresult
					// (таким образом мы не привязываемся к порядку полей в запросе)
					for idx, cname := range r.colnames {
						if lname == string(cname.Name) {
							labelValues[j] = row[idx].String
						}
					}
				}

				// игнорируем пустые значения, это NULL - нас они не интересуют
				if row[i].String == "" {
					log.Debugf("got empty value")
					continue
				}

				// получаем значение метрики (string) и конвертим его в подходящий для прометеуса float64
				v, err := strconv.ParseFloat(row[i].String, 64)
				if err != nil {
					log.Warnf("skip collecting metric: %s", err)
					continue
				}

				idx, err := lookupDesc(descs, string(colname.Name))
				if err != nil {
					log.Warnf("skip collecting metric: %s", err)
					continue
				}

				//отправляем метрику в прометеус
				ch <- prometheus.MustNewConstMetric(descs[idx].desc, descs[idx].valueType, v, labelValues...)
			}
		}
	}

	return nil
}
