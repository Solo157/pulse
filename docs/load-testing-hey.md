# Нагрузочные проверки через hey: масштабирование, хайлоад, отказоустойчивость

Все команды — на хосте стенда (там же, где крутится `docker compose`),
инструмент `hey` уже установлен (`/usr/bin/hey`, Ubuntu-пакет из universe,
апстрим — `github.com/rakyll/hey`). На другой Ubuntu-машине:
`apt install hey`; иначе — `go install github.com/rakyll/hey@latest`
или релизный бинарник из того же репозитория.

Доступ к Grafana/Prometheus — только с хоста стенда (порты опубликованы на
127.0.0.1, решение по защите периметра P1-8):

| Инструмент | Адрес | Логин | Пароль |
|---|---|---|---|
| Grafana | http://localhost:3000 | `admin` | `change-me` (значение из `.env`, ключ `GF_ADMIN_PASSWORD`) |
| Prometheus | http://localhost:9090 | — (без авторизации) | — |

Дашборд: папка **Pulse** → «Pulse overview (8 метрик)».

## Подготовка пользователя для нагрузки

`hey` умеет базовую авторизацию флагом `-a`; логином ingest является `userId`
(не email). Регистрация:

```bash
EMAIL="hey-$(date +%s)@pulse.dev"; PASS='Passw0rd!'
USER_ID=$(curl -s -X POST localhost/auth/register -H 'Content-Type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASS\"}" | python3 -c 'import sys,json;print(json.load(sys.stdin)["userId"])')
```

## Два режима нагрузки — и почему это важно

Сервис приёма требует заголовок `Idempotency-Key`, а `hey` шлёт **одинаковые**
запросы во всём прогоне. Отсюда два разных смысла одной команды вида
`hey -n 10000 -c 1 -q 20 -m POST …`:

1. **Режим «дубликат» (одна команда, один ключ).** Первый запрос пишется в
   конвейер, остальные 9999 — это повтор ключа: сервис отдаёт прежний ответ,
   в Kafka ничего не уходит. Это честная проверка HTTP-слоя (nginx,
   metric-service, журнал), но НЕ проверка пропускной способности конвейера.
2. **Режим «пишущий» (настоящая нагрузка на конвейер).** Каждому запросу нужен
   свой ключ — используйте обёртку `scripts/hey-load.sh` (или `loadgen.py`,
   у которой есть сверка с ClickHouse и лагом):

   ```bash
   ./scripts/hey-load.sh hey-1@pulse.dev 'Passw0rd!' 20 300   # 20 запросов/с, 5 минут
   ```

Команда записи (одинаковая во всех режимах):

```bash
hey -n 10000 -c 1 -q 20 -m POST \
  -a "$USER_ID:$PASS" \
  -H "Idempotency-Key: $(uuidgen)" \
  -T application/json \
  -d '{"metrics":[{"name":"temperature","value":42.0,"timestamp":"2026-11-09T12:00:00Z"}]}' \
  "http://localhost/users/$USER_ID/metrics"
```

Честное ограничение: `-d` статичен, поэтому во всём прогоне одна и та же
метка времени точки. В ClickHouse это не потеря (движок сворачивает
одинаковые точки), но сверять «принято = записано» счётчиком строк нельзя —
сверяйте по журналу приёмов (`metric.ingest_idempotency`, см. вывод обёртки)
или используйте `loadgen.py`, который меняет timestamp каждый запрос.

## Масштабирование (scale up/down под нагрузкой)

Запускаете пишущий режим (или `LOAD_RPS=… ./loadgen.py`), параллельно:

```bash
docker compose up -d --scale metric-service=3     # горизонтальное масштабирование входа
docker compose up -d --scale storage-service=2    # потребители записи
docker compose up -d --scale processing-service=2 # обработчики инцидентов
docker compose up -d --scale metric-service=1     # обратно
```

Что смотреть в дашборде и что значит:

| Панель | Норма при scale-up | Аномалия |
|---|---|---|
| «HTTP Request Rate» | трафик равномерно раскладывается по новым копиям (`job`/`instance`) | весь трафик на одной копии — nginx не переобходился |
| «API Latency (p95)» | держится или улучшается | растёт при добавлении копий — узкое место ниже по потоку |
| «Kafka Consumer Lag by group/partition» | при scale storage/processing лаг группы перестает расти | лаг группы продолжает расти — параллелизма не хватает |

Ограничение, которое надо помнить: у топиков `metrics`/`incidents` по **2
партиции**, поэтому 3-я копия storage/processing встанет в группу «в холостом
ходу» (0 партиций) — это не дефект, а потолок параллелизма DEMO-топика
(замер P1-4, `docs/results-et7.md`).

## Хайлоад / ступенчатый рост (режим «дубликат», рост темпа)

```bash
for Q in 20 50 100 200; do
  echo "== $Q qps =="; hey -n 2000 -c 1 -q $Q -m POST -a "$USER_ID:$PASS" \
    -H "Idempotency-Key: fixed-high-load-key" -T application/json \
    -d '{"metrics":[{"name":"temperature","value":42.0,"timestamp":"2026-11-09T12:00:00Z"}]}' \
    "http://localhost/users/$USER_ID/metrics" | grep -E "requests/s|95%|5xx"; sleep 10
done
```

Смотрите «API Latency (p95)» и «HTTP Request Rate» (по сервисам) дашборда;
JVM heap и CPU на дашборд не вынесены (решение оператора: дашборд = 8 метрик),
но доступны в Prometheus: `jvm_memory_used_bytes{area="heap"}`,
`process_cpu_usage`. Где первой упрётся латентность — то и потолок. Для пишущего хайлоада — та же ступень в `scripts/hey-load.sh`
с ростом rps (помните про потолок потребителей 2 копии на топик).

## Отключение звеньев конвейера: команды + панели

Общее правило: **приём метрик не зависит только от ClickHouse**; всё остальное
отказывается честно. Перед каждым опытом — включённая нагрузка (`hey -n 2000
-z 90s …` или обёртка).

| Звено | Выключить | Вернуть | Что увидите на панелях |
|---|---|---|---|
| storage-service (запись в CH) | `docker compose stop storage-service` | `docker compose start storage-service` | лаг `storage-group` растёт (панель «Kafka Consumer Lag»); ingest и HTTP — без изменений; «Metric Storage Latency (p95)» новых наблюдений не получает; после старта лаг дренажируется до нуля |
| processing-service | `docker compose stop processing-service` | `start` | лаг группы обработки (`processing-metrics`) растёт; «Metric Storage Latency» живёт (CH пишет); «Processing Throughput» падает до 0; «Incident Detection Latency (p95)» замирает |
| notification-service | `docker compose stop notification-service` | `start` | лаг `notification-group` растёт (видно на lag-панели); приём и запись не затронуты; статус доставки — только в БД (`metric.notifications`) |
| ClickHouse | `docker compose stop clickhouse` | `docker compose start clickhouse` | ingest остаётся 202 («Incoming Throughput» живёт); лаг `storage-group` растёт; «HTTP Error Rate» read-API честно деградит: всплеск доли [45]xx (таймаут ~3 с TimeLimiter, затем быстрые 503 CircuitBreaker) |
| Kafka: брокер kafka1 | `docker kill kafka1` | `docker compose start kafka1` | при RF2/minISR=2 запись в топики встаёт → ingest **отдаёт 5xx**: «HTTP Error Rate» → 1, «Incoming Throughput» падает в 0 — принятое решение оператора (ингест зависит от Kafka, outbox отменён); консьюмеры доедают накопленное; после старта всё восстанавливается |
| PostgreSQL | `docker compose stop postgres` | `start` | деградируют auth/ingest/read (5xx): «HTTP Error Rate» вверх, «HTTP Request Rate» проседает; Kafka и ClickHouse живут; после возврата сервисы восстанавливаются сами |
| копия metric-service под нагрузкой | `docker kill --signal=KILL $(docker compose ps -q metric-service | tail -1)` | `docker compose up -d metric-service` | часть запросов первым заходом ловит 502 (nginx узнаёт реплики из DNS — не health-check), повтор nginx спасает; сверяйте потери: журнал COMPLETED должен равняться принятию F1 (`docs/fault-injection.md`) |

Лаг вручную: `docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh
--bootstrap-server localhost:9092 --describe --all-groups`.

## Восемь метрик: смысл и расчёт

Prometheus-арифметика stateless: сервисы лишь отдают счётчики/таймеры с
`/actuator/prometheus` (scrape 10 с); «в секунду» и перцентили считаются
запросом. `rate(m[1m])` — средняя производная счётчика за окно (рестарт копии
обнуляет счётчик, `rate` это учитывает); `histogram_quantile(0.95, …)`
интерполирует квантиль по ведрам `_bucket` — точность ограничена шириной
ведра, а сами вёдра включены `percentiles-histogram` в yml сервисов. Метку
`service` на HTTP-панелях ставит relabel Prometheus (`prometheus.yml`), не
приложение.

| # | Панель / запрос | Что измеряет | Кто снимает |
|---|---|---|---|
| 1 | Incoming Throughput — `sum(rate(pulse_ingest_accepted_total[1m]))` | принятые **точки**/с (не пакеты): инкремент `points.size()` только после Kafka-ack, до него — 202 | metric-service, `MetricIngestService` |
| 2 | Processing Throughput — `sum(rate(pulse_processing_points_total[1m]))` | точки/с, прогнанные через правила; разрыв 1−2 = очередь группы обработки | processing-service, `IncidentEvaluator` |
| 3 | HTTP Request Rate — `sum by (service)(rate(http_server_requests_seconds_count[1m]))` | RPS сервиса (встроенный таймер Spring Boot, метки uri/status); видно балансировку nginx по копиям | все сервисы |
| 4 | HTTP Error Rate — доля `{status~"[45].."}` | доля 4xx/5xx (0..1) по всему стенду; при живой Kafka ~0, при `kill kafka1` честная 1. При нулевом трафике 0/0=NaN (пусто) | все сервисы |
| 5 | API Latency (p95) — `histogram_quantile(0.95, …http…bucket[5m])` | только **синхронный** HTTP-ответ (FR-1): инжест отвечает сразу после Kafka-ack; хвост конвейера — это №7/№8, поэтому «долгое» хранилище здесь не видно | все сервисы |
| 6 | Kafka Consumer Lag — `kafka_consumergroup_group_lag` | непрочитанные **сообщения** (не секунды) по каждой группе/партиции: log-end offset − committed offset по Admin API | `kafka-lag-exporter` (сам в группы не вступает) |
| 7 | Metric Storage Latency (p95) — `pulse_e2e_clickhouse_visible_*` | FR: от метки `acceptedAt` (ставит metric-service при приёме) до успешной вставки батча в ClickHouse. Время точки (`sampleTime`) не входит. При остановленном storage наблюдения не появляются (панель гаснет; рост — это лаг №6) | storage-service, `MetricEventConsumerListener` |
| 8 | Incident Detection Latency (p95) — `pulse_e2e_incident_published_*` | FR: от **времени точки** (`sampleTime`) до Kafka-ack события `IncidentDetected`; только генерация нарушения, без закрытий. Старый timestamp законно раздувает метрику, timestamp в будущем даёт отрицательную длительность и Micrometer молча выбрасывает наблюдение (панель пустая — граблили) | processing-service, `IncidentPublisher` |

FR-ориентиры: №7 ≤ 5 с, №8 ≤ 10–20 с (здоровый стенд — доли секунды/десятки
секунд при лаге). Панели доставки уведомлений намеренно нет (решение
оператора, набор = 8 метрик); факт доставки проверяется в БД:
`select status, count(*) from metric.notifications group by 1`.
