### PromQL Examples

#### Index
- [Count usage over time](#count-usage-over-time) (Продолжительность длинного события).
- [Max connected standby over period](#max-connected-standbys-over-period) (Максимальное количество подключенных реплик за период) 
- [Sum by existential metrics](#sum-by-existential-metrics) (Сумма по метрикам которые могут отсутствовать).

---

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
