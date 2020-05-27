package app

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/barcodepro/pgscv/service/internal/log"
	"github.com/barcodepro/pgscv/service/internal/stat"
	"github.com/barcodepro/pgscv/service/model"
	"github.com/barcodepro/pgscv/service/store"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"
	"os"
	"strconv"
	"strings"
	"time"
)

// collectPostgresMetrics collects metrics from Postgres service
// Сначала проверяем что сервис именно постгресовый, хотя это было выполнено в родительской функции.
// Подключаемся к БД и спрашиваем её версию. В зависимости от версии используем version-specific запросы.
// Следом собираем список баз - список нужен для сбора per-database статистики. На выходе будет либо список необходимых баз,
// либо список из одного элемента - БД которая была обнаружена при авто-обнаружении или заданная пользователем (маловероятный,
// но возможный сценарий).
// Далее сбрасываем флаги 'collectDone' - эти флаги используются чтобы не собирать обще-кластерную стату по несколько раз.
// Например pg_stat_database, pg_stat_replication и т.п.
// Также активируем статистику у которой наступило время для сбора. Как правило это редко-изменющаяся статистика типа параметров
// конфигурации, поэтому после ее сбора запускается таймер чтобы собирать её не в следующий раз, а через какое-то продолжительное
// время.
// Далее создаем конфиг для подключения и используя этот конфиг проходимся по списку БД, подключаемся к каждый из них и собираем
// статистику. После сбора обновляем счетчики последнего выполнения сбора.
func (e *prometheusExporter) collectPostgresMetrics(ch chan<- prometheus.Metric, service model.Service) (cnt int) {
	// paranoid check - check the service type, return if it is not a Postgres
	if service.ConnSettings.ServiceType != model.ServiceTypePostgresql {
		return 0
	}

	var dblist []string
	var version int // service version (depending on version, the used queries may vary)

	db, err := store.NewDB(service.ConnSettings.Conninfo)
	if err != nil {
		e.totalFailed++
		log.Warnf("collect failed %d/%d: %s; skip collecting %s", e.totalFailed, exporterFailureLimit, err, service.ServiceID)
		return 0
	}

	// check Postgres version and adjust queries depending on the version
	if err := db.Conn.QueryRow(context.Background(), pgVersionNumQuery).Scan(&version); err != nil {
		db.Close()
		e.totalFailed++
		log.Warnf("collect failed %d/%d: %s; skip collecting %s", e.totalFailed, exporterFailureLimit, err, service.ServiceID)
		return 0
	}
	adjustQueries(e.statCatalog, version)

	// get the list of databases from those need to get metrics (all Postgres databases have per-database metrics)
	dblist, err = db.GetDatabases()
	if err != nil {
		log.Warnf("failed to get list of databases: %s; use default database name %s,", err, db.Config.Database)
		dblist = []string{db.Config.Database}
	}

	// close current DB connection, next we will connect to particular databases
	db.Close()

	var now = time.Now()
	for i, desc := range e.statCatalog {
		if desc.StatType == "postgres" {
			// Before start the collecting, resetting all 'collectDone' flags. The 'collectDone' flag used for marking that
			// the shared stats is already collected to avoid repetitive collection of those stats.
			e.statCatalog[i].collectDone = false

			// Activate all expired stats descriptors. Use now() snapshot to avoid partially enabled descriptors
			if desc.Interval > 0 && now.Sub(desc.LastFired) >= desc.Interval {
				e.statCatalog[i].ActivateDescriptor()
			}
		}
	}

	// Create a DB connection config crafted by 'pgx' driver. Driver rejects manually created configs.
	// This config need to connecting to Postgres databases.
	config, err := pgx.ParseConfig(service.ConnSettings.Conninfo)
	if err != nil {
		log.Warnf("skip collecting stats for %s, failed to parse postgresql conninfo: %s", service.ServiceID, err)
		return 0
	}

	// Run collecting round, go through databases list and collect required statistics
	for _, dbname := range dblist {
		// swap database to target
		config.Database = dbname

		db, err := store.NewDBConfig(config)
		if err != nil {
			log.Warnf("collect failed: %s; skip collecting for dbname %s", err, dbname)
			continue
		}

		// get all necessary statistics
		n := e.getDBStat(db, ch, service.ConnSettings.ServiceType, version)
		cnt += n
		db.Close()
	}

	// After collecting, update time of last executed. Don't update times inside the collecting round, because that might
	// cancel collecting non-oneshot statistics. Use now() snapshot to use the single timestamp in all descriptors.
	now = time.Now()
	for i, desc := range e.statCatalog {
		if desc.StatType == "postgres" && desc.collectDone {
			e.statCatalog[i].LastFired = now
		}
	}
	return cnt
}

// collectPgbouncerMetrics
func (e *prometheusExporter) collectPgbouncerMetrics(ch chan<- prometheus.Metric, service model.Service) (cnt int) {
	// paranoid check - check the service type, return if it is not a Pgbouncer
	if service.ConnSettings.ServiceType != model.ServiceTypePgbouncer {
		return 0
	}

	var now = time.Now()
	for i, desc := range e.statCatalog {
		if desc.StatType == "pgbouncer" {
			// Before start the collecting, resetting all 'collectDone' flags
			e.statCatalog[i].collectDone = false

			// Activate all expired descriptors. Use now() snapshot to avoid partially enabled descriptors
			if desc.Interval > 0 && now.Sub(desc.LastFired) >= desc.Interval {
				e.statCatalog[i].ActivateDescriptor()
			}
		}
	}

	// Pgbouncer has a single database, connect to it and collect required statistics
	db, err := store.NewDB(service.ConnSettings.Conninfo)
	if err != nil {
		e.totalFailed++
		log.Warnf("collect failed %d/%d: %s; skip collecting %s", e.totalFailed, exporterFailureLimit, err, service.ServiceID)
		return 0
	}

	// собираем стату БД, в зависимости от типа это может быть баунсерная или постгресовая стата
	n := e.getDBStat(db, ch, service.ConnSettings.ServiceType, 0)
	cnt += n
	db.Close()

	// After collecting, update time of last executed.
	// Don't update times inside the collecting round, because that might cancel collecting non-oneshot statistics.
	// Use now() snapshot to use the single timestamp in all descriptors.
	now = time.Now()
	for i, desc := range e.statCatalog {
		if desc.StatType == "pgbouncer" && desc.collectDone {
			e.statCatalog[i].LastFired = now
		}
	}
	return cnt
}

// getDBStat collects metrics from the connected database
// задача функции собрать стату в зависимости от потребности - шаредную или приватную.
// Шаредная стата описывает кластер целиком, приватная относится к конкретной базе и описывает таблицы/индексы/функции которые принадлежат этой базе
// Для сбора статы обходим все имеющиеся источники и пропускаем ненужные. Далее выполняем запрос ассоциированный с источником и делаем его в подключение.
// Полученный ответ от базы оформляем в массив данных и складываем в общее хранилище в котором собраны данные от всех ответов, когда все источники обшарены возвращаем наружу общее хранилище с собранными данными
func (e *prometheusExporter) getDBStat(db *store.DB, ch chan<- prometheus.Metric, itype string, version int) (cnt int) {
	// обходим по всем источникам
	for i, desc := range e.statCatalog {
		if desc.StatType != itype {
			continue
		}
		// Skip inactive descriptors (schedule is not expired yet)
		if !desc.IsDescriptorActive() {
			continue
		}
		// Skip collecting if statistics is oneshot and already collected (in the previous database)
		if desc.collectDone && desc.collectOneshot {
			continue
		}

		log.Debugf("start collecting %s", desc.Name)

		// обрабатываем статки с пустым запросом
		if desc.QueryText == "" {
			if n, err := getPostgresDirInfo(e, db, ch, desc.Name, version); err != nil {
				log.Warnf("skip collecting %s: %s", desc.Name, err)
			} else {
				cnt += n
				e.statCatalog[i].collectDone = true
			}
			continue
		}

		// check pg_stat_statements availability in this database
		if desc.Name == "pg_stat_statements" && !db.IsPGSSAvailable() {
			log.Debug("skip collecting pg_stat_statements in this database")
			continue
		}

		rows, err := db.Conn.Query(context.Background(), desc.QueryText)
		if err != nil {
			log.Warnf("skip collecting %s, failed to execute query: %s", desc.Name, err)
			continue
		}

		var container []sql.NullString
		var pointers []interface{}

		var noRows = true
		for rows.Next() {
			noRows = false
			colnames := rows.FieldDescriptions()
			ncols := len(colnames)

			pointers = make([]interface{}, ncols)
			container = make([]sql.NullString, ncols)

			for i := range pointers {
				pointers[i] = &container[i]
			}

			err := rows.Scan(pointers...)
			if err != nil {
				log.Warnf("skip collecting %s, failed to scan query result: %s", desc.Name, err)
				continue // если произошла ошибка, то пропускаем эту строку и переходим к следующей
			}

			for c, colname := range colnames {
				// Если колонки нет в списке меток, то генерим метрику на основе значения [row][column].
				// Если имя колонки входит в список меток, то пропускаем ее -- нам не нужно генерить из нее метрику, т.к. она как метка+значение сама будет частью метрики
				if !stringsContains(desc.LabelNames, string(colname.Name)) {
					var labelValues = make([]string, len(desc.LabelNames))

					// итерируемся по именам меток, нужно собрать из результата-ответа от базы, значения для соотв. меток
					for i, lname := range desc.LabelNames {
						// определяем номер (индекс) колонки в PGresult, который соотв. названию метки -- по этому индексу возьмем значение для метки из PGresult
						// (таким образом мы не привязываемся к порядку полей в запросе)
						for idx, cname := range colnames {
							if lname == string(cname.Name) {
								labelValues[i] = container[idx].String
							}
						}
					}

					// игнорируем пустые строки, это NULL - нас они не интересуют
					if container[c].String == "" {
						log.Debugf("skip collecting %s_%s metric: got empty value", desc.Name, string(colname.Name))
						continue
					}

					// получаем значение метрики (string) и конвертим его в подходящий для прометеуса float64
					v, err := strconv.ParseFloat(container[c].String, 64)
					if err != nil {
						log.Warnf("skip collecting %s_%s metric: %s", desc.Name, string(colname.Name), err)
						continue
					}

					// отправляем метрику в прометеус
					ch <- prometheus.MustNewConstMetric(
						e.AllDesc[desc.Name+"_"+string(colname.Name)], // *prometheus.Desc который также участвует в Describe методе
						prometheus.CounterValue,                       // тип метрики
						v,                                             // значение метрики
						labelValues...,                                // массив меток
					)
					cnt++
				}
			}
		}

		rows.Close()

		if noRows {
			log.Debugf("no rows returned for %s", desc.Name)
			continue
		}

		e.statCatalog[i].collectDone = true

		// deactivate scheduler-based and oneshot descriptors (avoid getting the same stats in next loop iteration)
		if e.statCatalog[i].Interval > 0 && e.statCatalog[i].collectOneshot {
			e.statCatalog[i].DeacivateDescriptor()
		}

		log.Debugf("%s collected", desc.Name)
	}

	e.totalFailed = 0
	return cnt
}

// adjustQueries adjusts queries depending on PostgreSQL version
func adjustQueries(descs []statDescriptor, pgVersion int) {
	for _, desc := range descs {
		switch desc.Name {
		case "pg_stat_replication":
			switch {
			case pgVersion < 100000:
				desc.QueryText = pgStatReplicationQuery96
			}
		case "pg_replication_slots":
			switch {
			case pgVersion < 100000:
				desc.QueryText = pgReplicationSlotsQuery96
			}
		case "pg_wal_directory":
			switch {
			case pgVersion < 100000:
				desc.QueryText = pgStatWalSizeQuery96
			}
		case "pg_schema_sequence_fullness":
			if pgVersion < 100000 {
				desc.StatType = model.ServiceTypeDisabled
			}
		}
	}
}

// getPostgresDirInfo evaluates mountpoint of Postgres directory
func getPostgresDirInfo(e *prometheusExporter, db *store.DB, ch chan<- prometheus.Metric, target string, version int) (cnt int, err error) {
	var dirpath string
	if err := db.Conn.QueryRow(context.Background(), `SELECT current_setting('data_directory')`).Scan(&dirpath); err != nil {
		return cnt, err
	}
	switch target {
	case "pg_wal_directory":
		if version >= 100000 {
			dirpath = dirpath + "/pg_wal"
		} else {
			dirpath = dirpath + "/pg_xlog"
		}
	case "pg_log_directory":
		var logpath string
		if err := db.Conn.QueryRow(context.Background(), `SELECT current_setting('log_directory') WHERE current_setting('logging_collector') = 'on'`).Scan(&logpath); err != nil {
			return cnt, err
		}
		if strings.HasPrefix(logpath, "/") {
			dirpath = logpath
		} else {
			dirpath = dirpath + "/" + logpath
		}
	}

	mountpoints := stat.ReadMounts()
	realpath, err := stat.RewritePath(dirpath)
	if err != nil {
		return cnt, err
	}

	parts := strings.Split(realpath, "/")
	for i := len(parts); i > 0; i-- {
		if subpath := strings.Join(parts[0:i], "/"); subpath != "" {
			// check is subpath a symlink? if symlink - dereference and replace it
			fi, err := os.Lstat(subpath)
			if err != nil {
				return cnt, err
			}
			if fi.Mode()&os.ModeSymlink != 0 {
				resolvedLink, err := os.Readlink(subpath)
				if err != nil {
					return cnt, fmt.Errorf("failed to resolve symlink %s: %s", subpath, err)
				}

				if _, ok := mountpoints[resolvedLink]; ok {
					subpath = resolvedLink
				}
			}
			if device, ok := mountpoints[subpath]; ok {
				ch <- prometheus.MustNewConstMetric(e.AllDesc[target], prometheus.GaugeValue, 1, device, subpath, realpath)
				cnt++
				return cnt, nil
			}
		} else {
			device := mountpoints["/"]
			ch <- prometheus.MustNewConstMetric(e.AllDesc[target], prometheus.GaugeValue, 1, device, "/", realpath)
			cnt++
			return cnt, nil
		}
	}
	return cnt, nil
}
