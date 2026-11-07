# Pulse — учебное DEMO событие-ориентированного мониторинга

Высоконагружаемый приём метрик с последующим обнаружением инцидентов по пороговым
правилам и доставкой уведомлений. Реализация по плану `implementation.md`
(статусы этапов — там же, решения — в `docs/decisions.md`).

## Архитектура

```
client ──> nginx :80 ──> metric-service ──> Kafka [metrics] ──> storage-service ──> ClickHouse
                                 │
                                 └──> Kafka ──> processing-service ──> [incidents] ──> notification-service ──> ДЕМО-канал (self HTTP)
                                                  │                                        │
                                            PostgreSQL ────────────────────────────> PostgreSQL
                                          (метрики: users, правила, инциденты, состояния)   (metric.notifications)
```

| Компонент | Роль | Порт (внутр.) |
|---|---|---|
| `nginx` | единственная точка входа клиентов | `:80` на хосте |
| `metric-service` | регистрация, batch ingest (`202` после подтверждения Kafka) | 8080 |
| `storage-service` | consumer `metrics` → ClickHouse | 8081 |
| `processing-service` | consumer метрик, пороги/инциденты, publishes `incidents` | 8082 |
| `notification-service` | consumer `incidents`, доставка + статусы в БД | 8083 |
| `kafka1..3` | KRaft, replication factor 2 для топиков приложений | 9092 |
| `postgres` | пользователи, правила, инциденты, состояния, уведомления | 5432 |
| `clickhouse` | история метрик | 8123 |

## Требования

- Docker + Docker Compose v2.
- JDK 21 и Maven 3.9 — только для сборки/тестов (образы Services собирает Docker из
  уже собранных jar либо multi-stage; для быстрых прогонов тестов нужен локальный JDK).

## Быстрый старт

```bash
# 1. Собрать все модули (тесты: unit + интеграционные, нужен Docker для Testcontainers)
mvn clean install

# 2. Поднять инфраструктуру и приложение
docker compose up -d

# 3. Дождаться healthy-статусов
docker compose ps
```

> Ограничение рабочей среды: если Maven не доверяет центру Maven-репозитория (repo.maven.apache.org)
> (MITM proxy), задавайте truststore:
> `export MAVEN_OPTS="-Djavax.net.ssl.trustStore=$HOME/m2-truststore.jks -Djavax.net.ssl.trustStorePassword=changeit"`.

## Как пользоваться (сквозной сценарий)

Пороговое правило `temperature > 80` (`temperature-high`) засеяно при старте.

```bash
# 1) Регистрация пользователя (пароль не короче 8 символов) → вернёт {"userId": "..."}
curl -s -X POST localhost/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"email":"demo@pulse.dev","password":"***"}'

# 2) Пакет метрик (Idempotency-Key обязателен; повтор ключа не создаёт второй batch)
curl -s -X POST localhost/users/<userId>/metrics \
  -H 'Content-Type: application/json' -H "Idempotency-Key: $(uuidgen)" \
  -d '{"metrics":[{"name":"temperature","value":85.0,"timestamp":"2026-11-06T10:00:00Z"}]}'
# → 202 {"eventId":"...","userId":"...","acceptedCount":1}

# 3) Значение выше порога → ProcessingService откроет инцидент
docker compose exec -T postgres psql -U pulse -d pulse -c \
  "SELECT id, status, opened_at, closed_at FROM metric.incidents ORDER BY opened_at DESC LIMIT 5"

# 4) NotificationService создаст уведомление и доставит его в ДЕМО-канал
#    (HTTP POST на собственный endpoint /internal/demo-receiver)
docker compose exec -T postgres psql -U pulse -d pulse -c \
  "SELECT source_event_id, status, destination, attempts FROM metric.notifications ORDER BY created_at DESC LIMIT 5"
docker compose logs notification-service --tail 20 | grep -i "ДЕМО-канал\|доставлено"

# 5) «Выздоровление» — значение ниже порога закроет инцидент (close-событие,
#    уведомление при этом НЕ создаётся)
curl -s -X POST localhost/users/<userId>/metrics \
  -H 'Content-Type: application/json' -H "Idempotency-Key: $(uuidgen)" \
  -d '{"metrics":[{"name":"temperature","value":20.0,"timestamp":"2026-11-06T11:00:00Z"}]}'
```

История метрик пишется в ClickHouse (`storage-service` consumer):

```bash
set -a && . .env   # креды из .env
curl -s --data-binary \
  "SELECT metricName, value, timestamp FROM pulse.metric_points WHERE userId='<userId>' ORDER BY timestamp DESC LIMIT 10" \
  "http://localhost:8123/?user=$CLICKHOUSE_USER&password=$CLICKHOUSE_PASSWORD"
```

## Чтение истории (read API)

Данные читает только владелец: Basic-аутентификация, где логин — `userId`, пароль —
пароль регистрации (минимум 8 символов). Чужой `userId` в пути — `403`, отсутствие или
ошибка учётных данных — `401`.

```bash
# История метрик (ClickHouse): опции metric, from, to, limit (default 100, max 1000)
curl -s "localhost/users/<userId>/metrics?metric=temperature&limit=10"   -u "<userId>:<пароль-регистрации>"
# [{"name":"temperature","value":85.0,"timestamp":"2026-11-06T10:00:00Z"}, ...]

# Инциденты (PostgreSQL)
curl -s "localhost/users/<userId>/incidents" -u "<userId>:<пароль-регистрации>"
# [{"userId":"...","ruleId":"temperature-high","status":"OPEN","value":85.0,...}]
```

Приём метрик (`POST /users/{userId}/metrics`) остаётся публичным по идентификатору —
по согласованию ДЕМО API-ключ для ingest не используется.

## Целевая система (архитектура, ресурсы, точки масштабирования)

Моно-стенд поднят `docker compose`; это уменьшенная, но функционально полная
мишень будущего скейла.

### Микросервисы (Spring Boot 3.5 / Java 21, все stateless)

| Сервис | Роль | Входы / выходы | Реплицируется |
|---|---|---|---|
| **nginx 1.27** | входная точка `:80`, round-robin по A-записям DNS (`resolver valid=1s`), `proxy_next_upstream error timeout http_502/503`, request-id сквозной | клиентский HTTP → `metric-service:8080` | да (stateless) |
| **metric-service** | регистрация, приём метрик (idempotency), аутентификация ingest и read-API (Basic, BCrypt), история/инциденты владельцу | HTTP `POST /users/{id}/metrics`, read API; продюсер топика `metrics`; читает PostgreSQL, ClickHouse | `--scale` |
| **storage-service** |consumer пакета `metrics` → идемпотентно пишет `ClickHouse pulse.metric_points` | Kafka `metrics` (batch listener, 500/опрос, ack-mode batch) | `--scale` (потолок = партиции топика) |
| **processing-service** | потребитель `metrics`: правила/пороги → инциденты в PostgreSQL + событие в топик `incidents` | Kafka `metrics` → `incidents`; PostgreSQL | `--scale` (потолок = партиции `metrics`) |
| **notification-service** | потребитель `incidents` → внешняя доставка (webhook-заглушка), статусы SENT/FAILED, идемпотентность | Kafka `incidents`; PostgreSQL | `--scale` (потолок = партиции `incidents`) |

### Хранилища

- **PostgreSQL 16** (`postgres:16`, сеть compose) — транзакционный слой, схема `metric`:
  `users` (пароли BCrypt), `idempotency` (+ продублирован Redis-ключом), `rules`, `incidents`,
  учётные таблицы сервисов. Точка роста: вертикально + read-реплики.
- **ClickHouse 24.8** (`clickhouse-server`) — временной слой, таблица `pulse.metric_points`,
  `ReplacingMergeTree(insertedAt) ORDER BY (userId, metricName, timestamp, eventId)`
  (дедуп по eventId на read). Растёт шардированием/репликами.
- **Redis 7** (`appendonly yes`) — задел под быстрый слой идемпотентности; в текущем коде
  не задействован (ключи живут в PostgreSQL).

### Kafka 3.9 (KRaft, без ZooKeeper)

- 3 брокера; **топики**: `metrics` (2 партиции, RF 2, `min.insync.replicas=2`,
  key = `userId` — порядок точек пользователя в одной партиции), `incidents`
  (1 партиция, RF 2, `min.insync.replicas=2`).
- **Продюсеры**: `acks=all` + `enable.idempotence=true` (ни потерь, ни дублей при повторах),
  `linger`/батч по дефолтам, publish-ack-timeout 5 s.
- **Консьюмеры**: `auto-offset-reset=earliest`, `max.poll.interval.ms=120000`,
  batch + ack-mode `batch` (commit после успешной записи), consumer-id стабильны через
  `metric-group/storage-group/processing-metrics/notification...` группы.
- **Что это значит для скейла/нагрузки**: потребительские группы линейно масштабируются
  только до числа партиций (2 для `metrics`), дальше — только вертикально или ре-партиционированием
  топика; RF2+min.insync2 = аптайм при отказе 1 брокера, но потеря обеих брокеров из пары
  реплик = остановка записи (ошибка `NOT_ENOUGH_REPLICAS`); для продакшена — RF≥3,
  `min.insync.replicas=2`, больше партиций, отдельный cluster для Kafka.

### Настройки стенда (env `.env` / compose)

`POSTGRES_*`, `CLICKHOUSE_*`, `KAFKA_*` (RF, репликация offset-топика), креды Grafana
`GF_ADMIN_*`. Порты: nginx `:80`, Prometheus `:9090`, Grafana `:3000`, БД внутри сети `pulse`.
Микросервисы слушают 8080 (metric), 8081/8082/8083 (storage/processing/notification);
Prometheus — dns_sd по этим портам, без рестарта при `--scale`.

### Наблюдаемые SLI (Grafana `pulse-overview`)

Бизнес: принятые/отклонённые метрики (по `outcome`), реплеи по idempotency, ошибки
публикации, точки, записанные в CH, обработанные processing-ом, уведомления SENT/FAILED.
Система: p95 латентность HTTP, heap/CPU реплик, uptime. Лаги Kafka — consumer-groups CLI
(штатный экспортёр лагов ещё не поднят — кандидат Этапа 9).

## Масштабирование (для demos по нагрузке, Этап 7)

Stateless-сервисы масштабятся репликами (Kafka-группы распределяют партиции):

```bash
docker compose up -d --scale metric-service=3 --scale storage-service=2
```

Замеры и методика нагрузочных прогонов: `docs/results-et7.md`; воспроизведение:
`./loadgen.py` (профили `baseline` / `step` / `burst`). Отказоустойчивость
(fault injection): `docs/fault-injection.md`, шаги сценариев — `scripts/chaos.sh`.

## Профили и окружение

- `.env` / `.env.example` — креды и настройки PostgreSQL/ClickHouse; compose подхватывает
  их автоматически (`.env.example` переименуйте в `.env` при нестандартных значениях).
- `docker compose --profile obs up -d` — поднимает observability-стек (Prometheus/Grafana).
- Prometheus: <http://localhost:9090> (targets — все 4 сервиса, `/actuator/prometheus`,
  discovery по DNS — реплики подхватываются без рестарта, порты 8080..8083).
- Grafana: <http://localhost:3000> (`GF_ADMIN_USER`/`GF_ADMIN_PASSWORD` из `.env`), папка
  **Pulse** → дашборд `pulse-overview`: бизнес-показатели (ingest/Kafka/storage/
  processing/notifications) и системные (HTTP latency p95, heap, CPU, uptime).

## Тесты и проверка

```bash
mvn clean install           # все модули: unit + интеграционные (Testcontainers)
```

Интеграционные тесты поднятия compose не требуют; им нужен только Docker.

Полный сброс стенда с чистыми данными (тома Kafka/PostgreSQL/ClickHouse):
`docker compose --profile obs down -v` (проверено воспроизводимym стартом).

## Статус работ

Реализованы Этапы 0–9 плана `implementation.md` (ingest, storage, инциденты,
уведомления со статусами SENT/FAILED и идемпотентностью, read API с проверкой
владельца, observability: метрики микросервисов в Prometheus + дашборд в Grafana,
нагрузочные прогоны и масштабирование — см. `docs/results-et7.md`, fault injection —
`docs/fault-injection.md`, финализация с итоговым прогоном с чистого состояния:
`result.md` (итог, находки, бэклог, runbook).
