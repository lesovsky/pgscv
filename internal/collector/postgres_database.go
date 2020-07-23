package collector

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
)

const databaseQuery = "SELECT COALESCE(datname, '__shared__') AS datname, xact_commit, xact_rollback FROM pg_stat_database"

type typedDesc struct {
	colname   string
	desc      *prometheus.Desc
	valueType prometheus.ValueType
}

type postgresDatabaseCollector struct {
	descs      []typedDesc
	labelNames []string
}

func NewPostgresDatabaseCollector(labels prometheus.Labels) (Collector, error) {
	var databaseLabelNames = []string{"database"}

	return &postgresDatabaseCollector{
		labelNames: []string{"datname"},
		descs: []typedDesc{
			{
				colname: "xact_commit",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "database", "xact_commit_total"),
					"The total number of transactions committed.",
					databaseLabelNames, labels,
				), valueType: prometheus.CounterValue,
			},
			{
				colname: "xact_rollback",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "database", "xact_rollback_total"),
					"The total number of transactions rolled back.",
					databaseLabelNames, labels,
				), valueType: prometheus.CounterValue,
			},
		},
	}, nil
}

func (c *postgresDatabaseCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	db, err := store.NewDB(config.ConnString)
	if err != nil {
		return err
	}

	rows, err := db.Conn.Query(context.Background(), databaseQuery)
	if err != nil {
		return err
	}

	var container []sql.NullString
	var pointers []interface{}

	for rows.Next() {
		colnames := rows.FieldDescriptions()
		ncols := len(colnames)

		pointers = make([]interface{}, ncols)
		container = make([]sql.NullString, ncols)

		for i := range pointers {
			pointers[i] = &container[i]
		}

		err := rows.Scan(pointers...)
		if err != nil {
			log.Warnf("skip collecting database stats: %s", err)
			continue // если произошла ошибка, то пропускаем эту строку и переходим к следующей
		}

		for i, colname := range colnames {
			// Если колонки нет в списке меток, то генерим метрику на основе значения [row][column].
			// Если имя колонки входит в список меток, то пропускаем ее -- нам не нужно генерить из нее метрику, т.к. она как метка+значение сама будет частью метрики
			if !stringsContains(c.labelNames, string(colname.Name)) {
				var labelValues = make([]string, len(c.labelNames))

				// итерируемся по именам меток, нужно собрать из результата-ответа от базы, значения для соотв. меток
				for j, lname := range c.labelNames {
					// определяем номер (индекс) колонки в PGresult, который соотв. названию метки -- по этому индексу возьмем значение для метки из PGresult
					// (таким образом мы не привязываемся к порядку полей в запросе)
					for idx, cname := range colnames {
						if lname == string(cname.Name) {
							labelValues[j] = container[idx].String
						}
					}
				}

				// игнорируем пустые строки, это NULL - нас они не интересуют
				if container[i].String == "" {
					log.Debugf("got empty value")
					continue
				}

				// получаем значение метрики (string) и конвертим его в подходящий для прометеуса float64
				v, err := strconv.ParseFloat(container[i].String, 64)
				if err != nil {
					log.Warnf("skip collecting %s metric: %s", c.descs[i].desc.String(), err)
					continue
				}

				idx, err := lookupDesc(c.descs, string(colname.Name))
				if err != nil {
					log.Warnf("skip collecting metric: %s", err)
					continue
				}

				//отправляем метрику в прометеус
				ch <- prometheus.MustNewConstMetric(c.descs[idx].desc, c.descs[idx].valueType, v, labelValues...)
			}
		}
	}

	rows.Close()

	return nil
}

func lookupDesc(descs []typedDesc, pattern string) (int, error) {
	for i, desc := range descs {
		if desc.colname == pattern {
			return i, nil
		}
	}
	return -1, fmt.Errorf("pattern not found")
}
