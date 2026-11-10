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

## Расшифровка E2E-панелей (FR)

| Панель | Что измеряет | FR |
|---|---|---|
| «Metric Storage Latency (p95)» | от принятия пакета точкой до появления в ClickHouse | ≤ 5 с |
| «Incident Detection Latency (p95)» | от времени точки до подтверждения события инцидента брокером | ≤ 10–20 с |

Панель «Kafka Consumer Lag by group/partition» — сколько сообщений не прочитано
каждым consumer group; рост = не справляемся, падение = дренаж. Задержка
доставки уведомлений Observability-дашборда намеренно не входит (решение
оператора, 8 метрик); факт доставки проверяется в БД:
`select status, count(*) from metric.notifications group by 1`.

Примечание про установку hey на другой машине: используйте тот же бинарник,
что лежит в `/usr/bin/hey` на этом стенде (копия не в deb-пакете), либо любой
собранный `hey` с тем же набором флагов, которые применяются здесь:
`-n -c -q -z -m -H -T -d -a -o csv`.
