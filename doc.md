# Pulse — технологическое описание системы

Учебная DEMO-система событие-ориентированного мониторинга: высоконагруженный
приём пакетов метрик, обнаружение инцидентов по пороговым правилам, доставка
уведомлений. Источник планов и решений — `implementation.md`, `docs/decisions.md`,
результаты прогонов — `docs/results-et7.md`, `docs/fault-injection.md`.

## 1. Состав и архитектура

```
client ──> nginx :80 ──> metric-service ──> Kafka [metrics] ──> storage-service ──> ClickHouse
                                 │                   │
                    PostgreSQL ◄─┤                   └──> processing-service ──> [incidents]
               (users, rules,    │                            │                     │
                idempotency,     └── read API: история и      ▼                     ▼
                incidents,       инциденты (ClickHouse +   PostgreSQL ◄── notification-service
                notifications)   PostgreSQL, сразу 503 при
                                неисправном хранилище)      (инциденты)      └─> ДЕМО-канал (HTTP-запрос на себя)
```

| Компонент | Роль | Внутренний порт |
|---|---|---|
| `nginx` | единственная точка входа, сам находит новые копии сервиса, повторяет запрос при ошибке | `:80` на хосте |
| `metric-service` | регистрация (BCrypt), ingest с подтверждением Kafka, read API владельца | 8080 |
| `storage-service` | читает `metrics` пачками, пишет точки в ClickHouse | 8081 |
| `processing-service` | читает `metrics`: пороги, открытие/закрытие инцидентов, публикует `incidents` | 8082 |
| `notification-service` | читает `incidents`: доставка + запись статуса в базу | 8083 |
| `kafka1,kafka2` — приём запросов, `kafka3` — управление; по две копии каждого топика, ждать подтверждения от обеих | 9092 |
| `postgres:16` | users / monitoring_rules / incident_state / incidents / notifications / ingest_idempotency (схема `metric`) | 5432 |
| `clickhouse:24.8` | `pulse.metric_points` — история метрик | 8123 |
| `redis:7` | развёрнут как задел под быстрый слой идемпотентности, в коде не задействован | 6379 |
| `prometheus` + `grafana` (profile `obs`) | сбор `metrics`-эндпоинтов, готовый дашборд | 9090 / 3000 |

Гарантии сквозные: **хотя бы одна доставка** на всех путях через Kafka (сервис
сдвигает отметку прочитанного только после успешной записи, а при ошибке
повторяет пачку через 2 с столько раз, сколько нужно) + **защита приёмников от повторов** (повторная отправка того же
сообщения не создаёт второй результат: таблица ключей приёма в PostgreSQL,
на ingest, PK `source_event_id` у уведомлений, `eventId` в `ORDER BY` у
`ReplacingMergeTree` в ClickHouse).

## 2. Технологический стек

| Слой | Версия |
|---|---|
| Java / Maven | JDK 21 / Maven 3.9 |
| Spring Boot | 3.5.5 (общий родитель `spring-boot-starter-parent`) |
| Spring Cloud (openfeign — демо-канал) | в `notification-service` |
| resilience4j | 2.4.0 (`resilience4j-spring-boot3`, только metric-service) |
| Spring Kafka | из BOM Boot 3.5 (Kafka client 3.8) |
| Kafka / PostgreSQL / ClickHouse / nginx брокеров | 3.9.0 (apache/kafka, KRaft) / 16 / 24.8 / 1.27-alpine |
| Тесты | JUnit 5 + Mockito + AssertJ; интеграционные — Testcontainers (Kafka, PG, CH) |

## 3. Модель данных

PostgreSQL, схема `metric` (создаётся `spring.sql.init` при старте сервисов):

| Таблица | Назначение | Ключ идемпотентности/поиска |
|---|---|---|
| `users` | аккаунты (email uniq, BCrypt-хеш) | `id UUID` |
| `ingest_idempotency` | ключ → eventId первого принятого пакета | PK `idempotency_key` |
| `monitoring_rules` | пороги (`temperature-high > 80` засеяна), `enabled` | кэш в processing на 30 с |
| `incident_state` | текущее состояние правила+пользователя (OPEN/closed_at) | уникальный ключ правила |
| `incidents` | журнал открытий/закрытий | — |
| `notifications` | статусы доставки (`PENDING/SENT/FAILED`, `attempts`) | PK `source_event_id` |

ClickHouse: `pulse.metric_points (userId, metricName, value, timestamp, eventId)`
— `ReplacingMergeTree`, `ORDER BY (userId, metricName, timestamp, eventId)`.
Если Kafka повторно отдала событие и точки записались два раза, лишнее убирает
сам движок таблицы при фоновом слиянии; чтобы и до слияния читалось по одному
разу, запросы read API выполняются с `FINAL`.

## 4. Внешние контракты

| Метод | Тело / заголовки | Успех | Ошибки |
|---|---|---|---|
| `POST /auth/register` | `{"email","password"}` (пароль ≥ 8) | `201 {"userId"}` | 400 (валидация), дубль email → 409 |
| `POST /users/{id}/metrics` | Basic (`userId:пароль`, владелец), `Idempotency-Key` обязателен, JSON `{metrics:[{name,value,timestamp}]}` | `202 {eventId, userId, acceptedCount}` | 401/403, 400 (лимиты, JSON), 422, 503 (Kafka/цепи) |
| `GET /users/{id}/metrics` | Basic, владелец; параметры адреса `metric`,`from`,`to`,`limit` — все необязательные | `200 [point…]` (из ClickHouse) | 401/403, 400 (неверные параметры), 503 сразу, если ClickHouse неисправен |
| `GET /users/{id}/incidents` | Basic, владелец; параметры `from`,`limit` | `200 [incident…]` (из PostgreSQL) | 401/403, 503 сразу при неисправной базе (цепь `pgRead`) |

Kafka-события (`com.pulse.contracts`, модуль `pulse-contracts`): `metrics` —
`MetricEvent{eventId, userId, schemaVersion, acceptedAt, requestId, points[{name,
value,timestamp}]}` (ключ = userId, ISO-8601 даты); `incidents` —
`IncidentDetected{eventId, userId, ruleId, incidentKey, close, metricName,
threshold, value, createdAt, sampleTime, schemaVersion, requestId}`. Ошибки REST — RFC 7807
`application/problem+json` с свойством `requestId` (MDC, сквозной `X-Request-Id`).

## 5. Отказоустойчивость

- **Ingest**: `202` выдаётся только после `acks=all` подтверждения брокеров
  (ожидание future с таймаутом `pulse.publish-ack-timeout` = 5 c); мёртвый
  Kafka → 503 с предложением повторить позже.
- **Обработка инцидентов**: переход состояния инцидента и запись события
  `IncidentDetected` в таблицу-очередь `metric.incident_outbox` фиксируются
  одной транзакцией PostgreSQL. Отдельный отправитель раз в секунду добирает
  неоправленные события (после сбоя Kafka или падения сервиса) с растущей
  паузой повтора; повторная отправка отдаёт то же событие с тем же `eventId`,
  потребители от него защищены. Опубликованные записи очищаются через
  `pulse.outbox-retention`.
- **Консьюмеры**: `auto-offset-reset=earliest`, commit пачки после записи,
  `DefaultErrorHandler` = бесконечные повторы через 2 с (Kafka/БД ждём, не теряем;
  отравленные сообщения десериализуются в dead-letter-логирование конвертером).
- **Чтение истории (metric-service)**: мгновенный отказ (fail-fast) — `TimeLimiter` 3 с обрывает зависший
  запрос (иначе драйвер ClickHouse держит поток ~30 с), CircuitBreaker `chRead`/`pgRead`
  переводит цепь в закрытое состояние «отказ» (open), если из последних 10 вызовов
  больше 60 % медленнее 3 с или больше 50 % с ошибкой: новые запросы получают 503
  моментально. Каждые 10 с цепь пропускает 2 пробных запроса (half-open), и при
  успехе работа восстанавливается сама.
- **Доставка**: синхронно, 3 попытки с паузой 1 с, терминальный статус всегда в PG.
- **nginx**: при ошибке, таймауте, 502 и 503 повторяет запрос на другую копию
  сервиса, даже для POST (`proxy_next_upstream ... non_idempotent`); новые копии
  сервисов находятся через DNS Docker сами, перезагрузка nginx не нужна.
- Результаты прогонов F1…F7: `docs/fault-injection.md`.

## 6. Параметры конфигурации

Все перечисленные настройки имеют значения по умолчанию в `application.yml`;
compose задаёт только адреса (`SPRING_DATASOURCE_URL`, `PULSE_CLICKHOUSE_*`,
`SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092`).

### metric-service (8080)

| Параметр | Значение | Назначение |
|---|---|---|
| `pulse.max-points-per-request` | 1000 | лимит точек в пакете |
| `pulse.max-request-body-bytes` | 1 MiB | лимит тела (страховка tomcat `max-swallow-size` 1MB) |
| `pulse.publish-ack-timeout` | 5s | ожидание ack Kafka до 503 |
| `spring.datasource.hikari.connection-timeout / validation-timeout` | 3000 / 2000 мс | пул соединений к PostgreSQL; настройки привязаны прямо к объекту пула (см. `ClickHouseConfig`) |
| `spring.clickhouse.hikari.connection-timeout` | 3000 мс | пул соединений к ClickHouse (чтение) |
| параметры в JDBC URL | PostgreSQL `connectTimeout=2&socketTimeout=5` с; ClickHouse `connect_timeout=2000&socket_timeout=10000` мс | драйвер отвечает быстрее, чем пул, — сбой заметен сразу |
| `spring.kafka.producer.acks` / `enable.idempotence` | all / true | код 202 означает: данные записаны во все копии, дубли при ретрах исключены |
| `resilience4j.timelimiter…timeout-duration` | 3s, cancel-running-future | жёсткий потолок запроса чтения |
| `resilience4j.circuitbreaker.{chRead,pgRead}` | window 10, min-calls 4, slow >3s / 60 %, fail >50 %, open 10s, half-open 2, авто-переход | fail-fast read |

### storage-service (8081)

| Параметр | Значение | Назначение |
|---|---|---|
| группа чтения | `storage-group` | — |
| `max-poll-records` | 500 | максимум событий за одно обращение к Kafka (согласование п. 9) |
| чтение пачкой, отметка пачкой | — | сдвиг отметки прочитанного после успешной вставки |
| `max.poll.interval.ms` / `session.timeout.ms` | 120000 / 15000 | — |
| `spring.datasource.*` | драйвер ClickHouse, пул 3000/2000 мс | путь записи |
| метрики | `pulse.storage.inserted_points`, `insert_failures`, `insert.duration` | — |

### processing-service (8082)

| Параметр | Значение | Назначение |
|---|---|---|
| группа чтения | `processing-group` | — |
| `pulse.rule-cache-ttl` (`PULSE_RULE_CACHE_TTL`) | 30s | как часто перечитывать правила из базы |
| `pulse.close-cooldown` (`PULSE_CLOSE_COOLDOWN`) | 60s | не закрывать инцидент чаще раза в минуту (защита от дребезга) |
| `pulse.publish-ack-timeout` | 5s | ack для `incidents` |
| `pulse.outbox-poll-interval` (`PULSE_OUTBOX_POLL_INTERVAL`) | 1s | как часто отправитель добирает очередь `metric.incident_outbox`; он же база паузы повтора (удваивается) |
| `pulse.outbox-batch-size` (`PULSE_OUTBOX_BATCH_SIZE`) | 100 | событий в одной порции отправки |
| `pulse.outbox-retention` (`PULSE_OUTBOX_RETENTION`) | 7d | срок хранения отправленных записей очереди |
| producer `acks=all` + idempotence | — | как ingest |
| метрики | `pulse.processing.points`, `pulse.processing.outbox.pending`, `pulse.processing.outbox.oldest-pending-seconds`, `pulse.processing.outbox.published/failed` | глубина и задержка очереди отправки событий |

### notification-service (8083)

| Параметр (env) | Значение | Назначение |
|---|---|---|
| `pulse.delivery-url` (`PULSE_DELIVERY_URL`) | `/internal/demo-receiver` (сам себе) | ДЕМО-канал, заменяется на реальный адрес |
| `pulse.delivery-timeout` (`PULSE_DELIVERY_TIMEOUT`) | 3s | один HTTP-вызов |
| `pulse.delivery-max-attempts` (`PULSE_DELIVERY_MAX_ATTEMPTS`) | 3 | число попыток |
| `pulse.delivery-retry-delay` (`PULSE_DELIVERY_RETRY_DELAY`) | 1s | пауза |
| группа чтения | `notification-group` | — |
| метрики | `pulse.notifications{status=sent/failed/duplicate}` | — |

### Общие для сервисов

`server.shutdown=graceful`; actuator `health,info,prometheus,metrics`;
теги метрик `application`, `instance` (`INSTANCE_ID` при `--scale`);
`logging.pattern.console` включает `[req=%X{requestId}]`; `com.pulse: INFO`.

### Инфраструктура (docker-compose)

- Kafka: 3 брокера KRaft, `REPLICATION_FACTOR/OFFSETS/TRANSACTION_STATE=2`,
  `MIN_INSYNC_REPLICAS=2`; одноразовый `kafka-init` идемпотентно создаёт
  `metrics`/`incidents` (2 партиции, RF=2, minISR=2) — без него auto-create даёт 1 партицию.
- nginx: `resolver 127.0.0.11 valid=1s`, `proxy_read_timeout 60s`, JSON-ошибки 503.
- `.env`: только реально интерполируемые переменные; `POSTGRES_PASSWORD=change-me` и
  `GF_ADMIN_PASSWORD=change-me` — обязательная замена вне DEMO.
- Масштабирование: `docker compose up -d --scale metric-service=N` (stateless,
  консьюмер-группы — потолок = число партиций).

## 7. Сборка и запуск

```bash
# сборка и все тесты (unit + Testcontainers; Docker обязателен)
mvn clean install
# в среде, где Maven не доверяет сертификату репозитория (трафик перехватывает
# промежуточный прокси), задайте доверенный сертификат:
# MAVEN_OPTS="-Djavax.net.ssl.trustStore=… -Djavax.net.ssl.trustStorePassword=…"

docker compose up -d                      # приложение
docker compose --profile obs up -d        # + prometheus/grafana
./loadgen.py                              # генератор нагрузки (env LOAD_*)
./scripts/chaos.sh                        # шаги воспроизведения F1…F7
docker compose down -v                    # полный сброс стенда
```

Ограничения DEMO (детали в `result.md`): PostgreSQL и ClickHouse — по одному
экземпляру, их отказ останавливает систему (отказоустойчивости нет);
потолок параллелизма консьюмеров — 2 партиции; kafka-LAG читается только CLI
вывод лагов в Prometheus — в плане развития); абсолютные времена ответов относятся
к этому стенду и не переносятся на боевое окружение.
