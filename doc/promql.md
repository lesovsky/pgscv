### PromQL Examples

#### Index
- [Get IO latencies](#get-io-latencies) (средняя latency IO запросов).
- [Count usage over time](#count-usage-over-time) (Продолжительность длинного события).
- [Max connected standby over period](#max-connected-standbys-over-period) (Максимальное количество подключенных реплик за период) 
- [Sum by existential metrics](#sum-by-existential-metrics) (Сумма по метрикам которые могут отсутствовать).
- [Get indexes size depending on their usage](#get-indexes-size-depending-on-their-usage) (Значения на основе значений другой метрики)
---

##### Get IO latencies
Чтобы получить latency IO запросов, нужно взять отношение времени выполнения всех запросов к количеству выполненных запросов - `latency = T / n`.
Обе метрики являются типом COUNTER поэтому считаем через rate() функцию.
Метрика времени выражена в секундах и удобнее перевести её в миллисекунды домножив результат на 1000.
Пример ниже показывает как взять latency для read запросов:
```
rate(node_disk_time_seconds_total{type="reads",device="sdb"}[5m]) / rate(node_disk_completed_total{type="reads", device="sdb"}[5m]) * 1000
```

##### Count usage over time
Есть процедура резервного копирования: 1) запускается с периодичностью; 2) имеет время начала и конца.

Как показать продолжительность задачи резервного копирования?

```
(sum_over_time(count(postgres_replication_lag_seconds{state="backup", lag="write"})[1d:1m] ) * 60) or vector(0)
``` 
- `postgres_replication_lag_seconds{state="backup", lag="write"}` - метрика по которой можно судить что выполняется бэкап, метрика непостоянная и присутствует только тогда когда выполняется резервное копирование.
- `count(...)[1d:1m]` - подсчитываем количество метрик в течение дня с минутным интервалом (по сути там ряд единиц на выходе, если не делается несколько бэкапов одновременно).
- `sum_over_time(...) * 60` - суммируем подсчитанные значения и переводим в секунды.
- `(...) or vector(0)` - если за дневной интервал не было бэкапов, то показать 0 вместо `no data`

Похожие примеры: продолжительность пользовательской сессии на сервере.

Ссылки: [PrometheusCountUsageOverTime](https://utcc.utoronto.ca/~cks/space/blog/sysadmin/PrometheusCountUsageOverTime)

##### Max connected standbys over period
Определить максимальное количество одновременно подключенных standby узлов за период времени:
```
max_over_time(count(postgres_replication_lag_bytes{lag="write"})[1d:5m]) or vector(0)
```
- `count(postgres_replication_lag_bytes{lag="write"})` - считаем количество метрик с фильтром по lag="write"
- `max_over_time(...[1d:5m])` - считаем за дневной период с интервалом 5 минут
- `or vector(0)` - если нет метрик то показываем хотя бы 0


##### Sum by existential metrics
Есть метрика `postgres_statements_time_total COUNTER` с меткой "mode", метка может принимать значения `total`, `executing`, `ioread`, `iowrite`. При это метрики с `mode=total` есть всегда, а с остальными значениями могут отсутствовать. Например если запросы не используют IO, то для них не будут генерироваться метрики с "mode=(ioread|iowrite)".

Как показать все запросы которые использовали IO в целом, не важно "ioread" или "iowrite". Также для этих запросов нужны остальные метаданные (datname, usename и т.п.).

```
sum by (datname,usename,queryid,query) (postgres_statements_time_total{mode=~"io.*"})
```
- `postgres_statements_time_total{mode=~"io.*"}` - фильтруем метрики по рег.выражению - нужны только те который начинаются с "io".
- `sum by (datname,usename,queryid,query) (...)` - суммируем с группировкой по набору полей. 

##### Get indexes size depending on their usage
Есть две метрики:
- `postgres_index_size_bytes_total` - размер индекса в байтах
- `postgres_index_scans_total` - количество сканирований по индексу

Нужно найти размеры неиспользуемых (0 сканирований) индексов.

```
(postgres_index_size_bytes_total) + on(datname, schemaname, relname, indexrelname) group_right() (postgres_index_scans_total{key="false"} == 0)
```
- `postgres_index_size_bytes_total` - отталкиваемся от размеров
- `postgres_index_scans_total{key="false"} == 0` - берем группу метрик со значение 0
- `(...) + on(datname, schemaname, relname, indexrelname) group_right() (...)` - делаем **сумму значений** двух групп метрик на основе меток `datname`, `schemaname`, `relname`, `indexrelname`.
- полученная группа будет содержать метрики присоединяемой группы, например `key="false"`
- учитывая что в присоединяемой группе значения метрик равны 0 это не будет влиять на размеры (в противном случае нужно было бы метрики второй группы умножать на 0)
