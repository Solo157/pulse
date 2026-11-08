# Журнал решений (DEMO Pulse)

Хронологический лог технических решений, принятых по ходу этапов `implementation.md`.
Семантика требований — в `implementation.md`; здесь — «как и почему реализовано».

## Этап 0–1 — каркас

- Инфраструктура в `docker-compose.yml`: Kafka KRaft 3 узла (kafka1..3), PostgreSQL 16,
  ClickHouse 24.8, Redis, Nginx как единственный вход клиентов (`:80`).
  Прикладные сервисы — один образ, масштабирование `docker compose up --scale metric-service=N`.
- Внутрисетевой bootstrap Kafka: `kafka1:9092,kafka2:9092` (не `29092` — это внешний порт хоста).
- `spring-boot-starter-data-redis` удалён из metric-service: идемпотентность ingest
  обеспечивает только PostgreSQL (`metric.ingest_idempotency`), лишний redis-healthindicator
  ломал healthcheck профиля без Redis.

## Этап 2 — ingest/storage

- `202 Accepted` возвращается только после подтверждения Kafka (`send().get(timeout)`) — at-least-once.
- Идемпотентность ingest: таблица `metric.ingest_idempotency` в PostgreSQL по `Idempotency-Key`
  (ON CONFLICT DO NOTHING — повтор пакета не создаёт второй batch).
- `clickhouse-jdbc` берётся без classifier `all` + явный `httpclient5` (конфликт версий HTTP-клиента).
- В интеграционных тестах ClickHouse поднимается `GenericContainer` с собственными реквизитами доступа
  (у `clickhouse/clickhouse-server:24.8` пароль `default`-пользователя задан в образе).

## Этап 3 — processing/incidents

- Один инцидент = один период нарушения: состояние в `metric.incident_state`
  (ключ `userId:ruleId`); повторное нарушение в открытом периоде — не новый инцидент.
- Закрытие (`close`) — только по «нормальной» точке после `cooldown`; по таймеру инцидент
  не закрывается (ограничение ДЕМО).
- Out-of-order точки отсекаются по времени сэмпла (`sample_time > last_sample_time`, строго).
- Публикация `IncidentDetected` ДО записи состояния = at-least-once; все потребители
  идемпотентны (см. Этап 4).
- Кеш правил в ProcessingService: TTL + ленивая первая загрузка (не `ApplicationReadyEvent` —
  он не публикуется в `@SpringBootTest`).
- Таблицы `metric.users`, `metric.monitoring_rules` — owner MetricService/скрипты compose-init;
  processing читает их в своих тестовых контейнерах собственным minimal-DDL.

## Этап 4 — notifications

- Delivery-канал ДЕМО = HTTP POST на собственный endpoint сервиса
  `/internal/demo-receiver` (`pulse.delivery-url`, по умолчанию `http://localhost:8083/...`;
  в тестах — адрес на `local.server.port`, в compose — self).
- `pulse.delivery-url` читается из `Environment` при КАЖДОЙ доставке, а не биндится в бине:
  `local.server.port` появляется только после старта веб-сервера теста.
- Идемпотентность: PK `metric.notifications.source_event_id` = `eventId` события Kafka,
  `INSERT ... ON CONFLICT DO NOTHING` — «чейнул вставку — тот и доставляет»
  (дубликат события не даёт второй доставки).
- Повторы синхронные: `pulse.delivery-max-attempts` (default 3) с паузой
  `pulse.delivery-retry-delay`; терминальный статус `SENT`/`FAILED` всегда пишется в БД —
  «проверяемый статус» этапа. Событие не зависает и не повторяется вечно.
- События `close` уведомление НЕ создают.
- Обработка ошибок listener: `FixedBackOff(2000, UNLIMITED)` —
  бесконечным циклом, лог с ошибкой; в ДЕМО это допустимо при PK-идемпотентности.
- Тестовый DDL: `test-users.sql` (минимум `metric.users` — owner MetricService) через
  `withInitScript` тестового Postgres-контейнера.

## Рабочая среда (ограничение дев-машины, не продукта)

- Maven TLS к repo.maven.apache.org не доверяет цепочке (MITM/LE cross-chain). Обход:
  `MAVEN_OPTS="-Djavax.net.ssl.trustStore=$HOME/m2-truststore.jks -Djavax.net.ssl.trustStorePassword=changeit"`
  (`.mvn/jvm.config` ненадёжен — truststore задаётся через MAVEN_OPTS всех mvn-команд).

## Этап 5 — read API

- Модель доступа ДЕМО (FR-4 «клиент читает только свои данные»): пароль при регистрации
  проверяется на минимум 8 символов и хранится BCrypt-хешем (`metric.users.password_hash`,
  идемпотентная миграция `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`). Тот же кред — Basic-
  аутентификация GET-эндпоинтов; login = `userId`, пароль = пароль регистрации.
  (Инвариант Et2 «API-ключ не используется» сохранён.)
- Контроль — лёгкий сервлет-фильтр `OwnerReadAuthFilter` (url `/users/*`, метод GET),
  а не Spring Security: нужна ровно одна проверка «запросил свои данные». Порядок:
  нет/битый Basic → 401 (+ WWW-Authenticate); пользователь не найден/неверный пароль → 401;
  аутентифицирован, но `{userId}` в пути чужой → 403. Ошибки — общий RFC 7807 с requestId.
- GET `/users/{userId}/metrics` — только свои (owner-фильтр по path и cred'у одновременно)
  из ClickHouse `pulse.metric_points FINAL`; параметры `metric/from/to/limit` (default 100,
  потолок 1000 как в ingest).
- GET `/users/{userId}/incidents` — из PostgreSQL `metric.incidents` (таблицу ведёт
  ProcessingService; MetricService читает только для отдачи истории).
- ClickHouse для чтения — второй `DataSource` (`spring.clickhouse.*`, env `PULSE_CLICKHOUSE_*`,
  `depends_on: clickhouse: service_healthy`).
  Объявление собственного DataSource bean'а отключает автоконфиг `DataSourceAutoConfiguration`
  (условие `@ConditionalOnMissingBean`), поэтому PostgreSQL-источник объявлен явно и помечен
  `@Primary` — иначе `spring.sql.init` уезжает в ClickHouse.
- Границы времени в CH-запросе — литерал `parseDateTimeBestEffort('<ISO-8601>')`: ClickHouse JDBC
  (SqlBasedPreparedStatement) подставляет параметры как текст, и `Timestamp.toString()` с пробелом
  ломает синтаксис. Значения форматируются из `Instant`, инъекция исключена.

## Fast-fail read-пути (закрытие находки F5 fault-injection)

- Read-запросы (`UserReadService.getHistory/getIncidents`) защищены связкой
  **resilience4j TimeLimiter + CircuitBreaker** (цепи `chRead`/`pgRead`, конфиг в
  `application.yml`): зависшая выборка принудительно обрывается по таймауту (3 с),
  сбои накапливаются в статистике выключателя; взведённый выключатель мгновенно
  отвечает `DataUnavailableException` → 503 RFC 7807; периодические пробы
  (half-open) проверяют восстановление (`automatic-transition`).
  Метрики `resilience4j_circuitbreaker_*` доступны на `/actuator/prometheus`
  (отдельная панель в Grafana — отложена, см. план развития в `result.md`).
- **Почему программно, а не аннотациями**: `@TimeLimiter` оборачивает уже вычисленный
  `CompletableFuture`, а блокирующий JDBC-вызов к моменту возврата из метода уже
  выполнен на потоке запроса (проверено смоуком: 30 с сохранялись). Выборка выполняется
  в изолированном пуле (8 потоков/очередь 32), future ограничивается
  `TimeLimiter.decorateCompletionStage` + shared `ScheduledExecutorService`; внешний
  `CircuitBreaker.executeCompletionStage` фиксирует и асинхронные сбои (в т.ч.
  `TimeoutException`) — проверено min-тестом и `ReadFailFastIntegrationTest`.
- **Что висит после обрыва**: сокет JDBC не прерывается, «сирота» доживает в пуле
  выборок — ограничено потоками и очередью; отказ очереди → 503. В проде потолок даёт и
  nginx `proxy_read_timeout`. Accepted trade-off демо.
- `hikari.connection-timeout=3s` на обоих источниках metric-service — полезный фон,
  но НЕ решает задачу (драйвер ClickHouse игнорирует connect_timeout при HTTP-висяке:
  замеренные 30 с оставались до появления TimeLimiter).
- Смоук (стенд, `docker compose stop clickhouse`): 1-е обращение ~1,1 с → 503, далее
  ~100 мс (цепь открыта), инциденты (PG, своя цепь) — 200 всюду; после старта CH и
  перезапуска metric-service — 200 сразу; state-метрики closed/open видны в Prometheus.

## Transactional outbox событий инцидентов (закрытие P0-1)

- Дефект: `IncidentEvaluator` публиковал `IncidentDetected` в Kafka сразу после
  перехода `metric.incident_state` (два независимых шага). Сбой между ними при
  недоступной Kafka терял событие навсегда: при повторной доставке пакета
  инцидент уже «открыт» — ветка публикации больше не выполнялась. Заявленная
  в javadoc гарантия (публикация ДО фиксации, потеря невозможна) была инвертирована.
- Решение (референс — transactional outbox): таблица `metric.incident_outbox`,
  запись события — в той же транзакции PostgreSQL, что и переход состояния.
  `IncidentEvaluator.process` помечен `@Transactional` (пакет = транзакция:
  откат откатывает и `registerSample`, поэтому повтор пакета не «съедается»
  фильтром out-of-order). Переходы `openIncident/closeIncident` получили условие
  по `open` и возвращают факт перехода — событие ставится в очередь только при
  реальном переходе (плюс защита от `ON CONFLICT (id) DO NOTHING`).
- `eventId` события генерируется один раз при постановке в очередь и является
  primary key записи outbox: повторные публикации байт-в-байт идентичны, а
  потребители already идемпотентны по `eventId`/`incidentKey`.
- Отправитель (`OutboxDrainer`, `@Scheduled`, отключаемый
  `pulse.outbox-scheduling-enabled`): порция через `UPDATE ... WHERE id IN (SELECT
  ... FOR UPDATE SKIP LOCKED LIMIT n) RETURNING` с держанием `lock_until`
  (переживает смерть отправителя), пауза повтора `outbox-poll-interval * 2^attempt`,
  чистка опубликованного по `outbox-retention`. `KafkaTemplate.send` при
  недоступном кластере отказывает синхронно (`max.block.ms=2s`, иначе висит 60 с) —
  `KafkaException` тоже считается неудачей порции.
- Метрики: `pulse.processing.outbox.pending`, `oldest-pending-seconds`,
  `published`, `failed`.

## Устойчивый ingest: outbox + журнал идемпотентности (закрытие P0-2)

- Дефект: `MetricIngestService` публиковал в Kafka до закрытия журнала
  идемпотентности и при любой ошибке публикации удалял запись `IN_PROGRESS`.
  Таймаут ack не доказывает незапись в Kafka — клиентский повтор мог создать
  второе событие с новым eventId; падение после ack, но до `markCompleted`,
  оставляло `IN_PROGRESS` навсегда (клиент получал 409 бесконечно).
- Решение: таблица `metric.ingest_outbox` + журнал в одной транзакции
  (`@Transactional` на `ingest`). eventId генерируется при создании приёма и
  является primary key очереди; HTTP 202 = устойчивый приём (пара записей в
  PostgreSQL), а не попытка отправки. Отправитель (`IngestOutboxDrainer`,
  порция `FOR UPDATE SKIP LOCKED` + `lock_until`, пауза `poll-interval * 2^n`)
  закрывает outbox и журнал одной транзакцией; если процесс упал ровно между
  ack и закрытием — `reconcileCompleted()` перед каждой порцией догоняет статус,
  вечного `IN_PROGRESS` нет.
- Контроль тела: `request_hash` (SHA-256 канонического JSON точек);
  повтор того же ключа с другим телом — 409 (раньше такое молча получало чужой
  accepted-ответ). Смена семантики ошибок: недоступная Kafka больше не 503
  (доставка задерживается); 503 остаётся только за неисправным PostgreSQL.
  Класс/обработчик `KafkaUnavailableException` удалены.
- Хранение: outbox — `pulse.outbox-retention` (7д), журнал — `pulse.idempotency-retention` (30д),
  чистка по расписанию. Метрики `pulse.ingest.outbox.*`.
- Побочное: `max.block.ms=2s` на продюсерах (метрика+обработка) — при мёртвом
  кластере синхронный отказ вместо зависания на 60 с.

## Измерение нагрузки в точках метрик (закрытие P0-3)

- Единица измерения — точки/с: `LOAD_BATCH_SIZE` точек в HTTP-payload, единая
  цель `LOAD_TARGET_POINTS_PER_SEC`, из неё строго HTTP RPS = target/batch.
  Прежние «RPS» этапы 7 фактически были точками/с при batch=1; прежние
  результаты помечены в `docs/results-et7.md` как RPS-замеры.
- offered / accepted / подтверждённые раздельны: planned_requests/planned_points
  — нагрузка; accepted_points — сумма acceptedCount ответов 202; персистентность —
  COUNT по ClickHouse и LAG Kafka отдельными командами оператора
  (`LOAD_CH_COUNT_CMD`/`LOAD_KAFKA_LAG_CMD` с `{user}`).
- Планировщик честный: ограниченная очередь работ, high-water mark и
  `dropped_jobs` в отчёте, `join()` до реальной отправки; молча не теряется ни
  одна задача.
- Idempotency-Key стабилен на логический batch и его retry — безопасно после
  P0-2 (журнал идемпотентности), network-retry того же batch не создаёт дублей.
- `shell_out` в генераторе — `check=True`: упавшая проверочная команда обязана
  быть ошибкой в отчёте, а не молчаливым «LAG=0» (на этом горели первые замеры:
  `kafka-consumer-groups.sh` отсутствует в PATH контейнера, полный путь —
  `/opt/kafka/bin/`).
- Результат замера 2026-11-08: ingest/Kafka принимают 60k точек/с; сквозной
  предел — storage≈6.5k, processing≈3–3.5k точек/с на инстанс; потерь и дублей
  нет (ровно 7.2M/7.2M). Подробности и ограничения — `docs/results-et7.md`.
