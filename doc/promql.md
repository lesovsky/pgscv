### PromQL Examples

#### Index
- [Count usage over time](#count-usage-over-time) (Продолжительность длинного события).
- [Sum by existential metrics](#sum-by-existential-metrics) (Сумма по метрикам которые могут отсутствовать).

---

##### Count usage over time
Есть процедура резервного копирования: 1) запускается с периодичностью; 2) имеет время начала и конца.

Как показать продолжительность задачи резервного копирования?

```
sum_over_time(count(postgres_replication_lag_seconds{state="backup", lag="write"})[1d:1m] ) * 60
``` 
- `postgres_replication_lag_seconds{state="backup", lag="write"}` - метрика по которой можно судить что выполняется бэкап, метрика непостоянная и присутствует только тогда когда выполняется резервное копирование.
- `count(...)[1d:1m]` - подсчитываем количество метрик в течение дня с минутным интервалом (по сути там ряд единиц на выходе, если не делается несколько бэкапов одновременно).
- `sum_over_time(...) * 60` - суммируем подсчитанные значения и переводим в секунды.

Похожие примеры: продолжительность пользовательской сессии на сервере.

Ссылки: [PrometheusCountUsageOverTime](https://utcc.utoronto.ca/~cks/space/blog/sysadmin/PrometheusCountUsageOverTime)

##### Sum by existential metrics
Есть метрика `postgres_statements_time_total COUNTER` с меткой "mode", метка может принимать значения `total`, `executing`, `ioread`, `iowrite`. При это метрики с `mode=total` есть всегда, а с остальными значениями могут отсутствовать. Например если запросы не используют IO, то для них не будут генерироваться метрики с "mode=(ioread|iowrite)".

Как показать все запросы которые использовали IO в целом, не важно "ioread" или "iowrite". Также для этих запросов нужны остальные метаданные (datname, usename и т.п.).

```
sum by (datname,usename,queryid,query) (postgres_statements_time_total{mode=~"io.*"})
```
- `postgres_statements_time_total{mode=~"io.*"}` - фильтруем метрики по рег.выражению - нужны только те который начинаются с "io".
- `sum by (datname,usename,queryid,query) (...)` - суммируем с группировкой по набору полей. 
