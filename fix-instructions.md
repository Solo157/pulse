# Pulse — задания ИИ-агенту на исправление по результатам аудита

Работай в существующем проекте. **Не переписывай систему с нуля, не меняй стек и не удаляй рабочие функции.** Сначала прочитай этот документ и текущую реализацию. Исправляй небольшими этапами. После каждого этапа запускай релевантные тесты и сообщай точные результаты.

## Общие правила

1. Сохраняй Java 21, Spring Boot, Maven multi-module, Kafka, PostgreSQL, ClickHouse, Docker Compose, Nginx.
2. Не заменяй Kafka на RabbitMQ и ClickHouse на другую БД без отдельного согласования.
3. Не делай архитектурных изменений, влияющих на гарантии доставки, схемы БД или публичный API, молча. Если есть несколько существенно разных вариантов — опиши варианты, trade-offs и запроси решение владельца.
4. Не утверждай, что тест прошёл, если он не запускался. Не подменяй интеграционный тест unit-тестом.
5. Не исправляй только комментарии: сначала исправь поведение, затем синхронизируй документацию.
6. Сохраняй совместимость контрактов событий либо вводи версионирование миграционно.
7. Для каждого исправления добавь тест, который воспроизводит исходный дефект.
8. Не добавляй бесконечные retry без backoff/observability и без стратегии poison messages.

---

# P0 — блокирующие исправления

## P0-1. Гарантированная публикация событий открытия/закрытия инцидентов

### Затронутые файлы
- `processing-service/src/main/java/com/pulse/processing/service/IncidentEvaluator.java`
- `processing-service/src/main/java/com/pulse/processing/service/IncidentPublisher.java`
- `processing-service/src/main/java/com/pulse/processing/state/IncidentStateRepository.java`
- `processing-service/src/main/java/com/pulse/processing/state/IncidentRowRepository.java`
- `processing-service/src/main/resources/db/schema.sql`
- `processing-service/src/main/resources/application.yml`
- соответствующие integration tests.

### Дефект
`IncidentEvaluator` сначала фиксирует `open=true`/`open=false` в PostgreSQL, а потом публикует `IncidentDetected`. Если Kafka publish не удался, повторная обработка входного metric event не повторяет событие, так как состояние уже изменено. В `IncidentPublisher` комментарий описывает обратную последовательность и не соответствует коду.

### Требуемое решение
Предпочтительный вариант — **transactional outbox в PostgreSQL**:
- в одной DB-транзакции менять `incident_state` и добавлять outbox event;
- отдельный publisher/worker читает непубликованные outbox events и публикует их в Kafka `incidents`;
- помечать событие опубликованным только после Kafka ack;
- повторная публикация должна быть безопасна;
- outbox publisher должен восстанавливаться после рестарта;
- обеспечить блокировку/claim записей несколькими репликами (например, `FOR UPDATE SKIP LOCKED` или эквивалент);
- использовать стабильный event ID, созданный при записи outbox, а не генерировать новый ID при каждой попытке отправки;
- предусмотреть retry/backoff и метрики возраста старейшего unpublished event / размера backlog.

**Не ограничивайся простой перестановкой `publish()` и `openIncident()`**: без общей транзакции остаётся окно потери события.

Перед реализацией outbox проверь текущие транзакционные границы JDBC и предложи минимальную схему. Если считаешь, что для текущего DEMO нужен более простой вариант, сначала представь владельцу сравнение outbox и альтернатив.

### Тесты/критерии приёмки
1. Kafka недоступна при открытии инцидента: состояние и outbox фиксируются атомарно; событие публикуется после восстановления Kafka без повторной входной метрики.
2. Процесс падает после commit БД, но до публикации: после рестарта outbox event доставляется.
3. Процесс падает после Kafka ack, но до отметки published: повторная отправка не создаёт второй бизнес-инцидент/уведомление.
4. То же для закрытия инцидента.
5. Две реплики processing-service не публикуют независимые разные события для одной записи outbox.
6. В логах/метриках можно увидеть retry, backlog и возраст oldest event.

## P0-2. Устойчивый idempotency ingest при неопределённом результате Kafka publish

### Затронутые файлы
- `metric-service/src/main/java/com/pulse/metric/service/MetricIngestService.java`
- `metric-service/src/main/java/com/pulse/metric/service/MetricEventPublisher.java`
- `metric-service/src/main/java/com/pulse/metric/repository/IngestIdempotencyRepository.java`
- `metric-service/src/main/resources/db/schema.sql`
- `metric-service/src/main/resources/application.yml`
- интеграционные тесты ingest.

### Дефект
После любого RuntimeException при publish сервис удаляет `IN_PROGRESS`. Но timeout ожидания Kafka ack не означает, что событие не было записано. Клиентский повтор может создать второе событие с новым eventId. Также crash после Kafka ack, но до `markCompleted`, оставляет claim в `IN_PROGRESS` навсегда.

### Требуемое решение
Рекомендуется transactional outbox для входящих metric batches:
- в одной транзакции создать idempotency operation и outbox event;
- стабильный operation/event ID привязать к ключу идемпотентности;
- клиентский retry с тем же ключом получает тот же результат/operation ID, не создавая новое событие;
- publisher доставляет outbox event в Kafka at-least-once;
- consumer side должна быть идемпотентной по стабильному event ID;
- хранить request hash/payload fingerprint, чтобы повтор с тем же ключом, но другим телом возвращал 409;
- определить поведение для незавершённой операции: вернуть 202/operation status либо согласованный ответ, но не оставлять вечный IN_PROGRESS без восстановления;
- retention idempotency records должен быть определён.

Если outbox не выбран, сначала покажи владельцу альтернативы и гарантии. Нельзя просто перестать удалять claim без механизма восстановления/lease и нельзя считать timeout доказательством неуспешной публикации.

### Тесты/критерии приёмки
1. Kafka принимает событие, но ack искусственно задержан за пределы HTTP timeout; повтор того же Idempotency-Key не создаёт второе логическое событие.
2. Процесс падает после фиксации операции и до публикации — после рестарта событие публикуется.
3. Процесс падает после Kafka ack и до обновления статуса — retry не создаёт новую операцию.
4. Два одновременных запроса с одинаковым ключом создают ровно одну операцию.
5. Один ключ + другое тело → 409.
6. Новый ключ → новая операция.
7. Успешный HTTP 202 соответствует устойчиво принятой операции, а не только попытке отправить сообщение.

## P0-3. Переделать loadgen на измерение points/s и batch size

### Затронутые файлы
- `loadgen.py`
- `docs/results-et7.md`
- `README.md`
- при необходимости `result.md`.

### Текущая проблема
Генератор всегда отправляет одну metric point в каждом HTTP запросе. Поэтому 50 RPS — около 50 points/s, а не 50 000 points/s.

### Требования
Добавить параметры:
- `LOAD_BATCH_SIZE` — количество точек в одном HTTP payload (включая 1 и 100);
- `LOAD_TARGET_POINTS_PER_SEC` либо однозначно определённый режим `LOAD_RPS × LOAD_BATCH_SIZE`;
- `LOAD_DURATION`;
- `LOAD_USERS`/concurrency;
- профили baseline, step, burst;
- генерацию разных timestamp/value, включая управляемую долю превышений правил;
- режим steady load и burst;
- стабильный Idempotency-Key на один логический batch; при retry того же batch ключ не меняется.

В итоговый JSON записывать:
- planned_requests, completed_requests;
- planned_points, accepted_points (по успешным batch response);
- errors по HTTP-кодам и transport failures;
- achieved_requests_per_sec;
- achieved_points_per_sec;
- batch_size, concurrency, duration;
- p50/p95/p99/max latency;
- время от начала теста до завершения drain lag;
- queue high-water mark и dropped/not-scheduled jobs, если есть;
- результаты проверки числа точек в ClickHouse и Kafka lag.

Генератор не должен молча терять задачи при переполнении очереди. Планировщик должен учитывать реальную отправку, а не только создавать backlog в локальной очереди. Явно разделить offered load, accepted throughput и processed throughput.

### Тесты/критерии приёмки
1. batch size 1 и 100 корректно формируют payload.
2. При batch=100 и 200 HTTP RPS целевая интенсивность составляет примерно 20 000 points/s.
3. При batch=100 и 600 HTTP RPS — примерно 60 000 points/s, если клиент/стенд способен.
4. В отчёте не путать HTTP RPS и points/s.
5. Провести ступени с ростом нагрузки, определить фактический предел стенда; если 20k/60k не достигнуты, так и написать.
6. Проверить количество принятых точек и отсутствие потерь/необъяснимых дублей.

Не обещай, что текущая VM выдержит 60k points/s. Задача — измерить предел и представить честные результаты.

## P0-4. Привести Kafka topology и документацию в соответствие

### Затронутые файлы
- `docker-compose.yml`
- `README.md`
- `result.md`
- `docs/decisions.md`
- `docs/fault-injection.md`.

### Требования
Сейчас:
- kafka1 и kafka2 — broker + controller;
- kafka3 — controller only;
- RF=2, minISR=2.

Это два data brokers и один controller-only node. При остановке одного broker запись с RF=2/minISR=2 прекращается. Для текущего DEMO можно оставить это, если документация честно объясняет ограничение.

Не менять топологию на три broker без согласования. Если владелец хочет тест переживания отказа одного broker с продолжением записи, предложить отдельный вариант с 3 data brokers, RF=3/minISR=2 и размещением на разных физических/виртуальных узлах. Объяснить, что три контейнера на одном хосте не дают защиты от отказа хоста.

### Приёмка
- Все комментарии, README, result.md и fault docs описывают одну и ту же топологию.
- F6 формулируется в соответствии с RF/minISR и фактическим результатом.
- Не называть controller-only node брокером, хранящим partition replicas.

## P0-5. Синхронизировать результаты fault injection с текущим кодом

### Затронутые файлы
- `docs/fault-injection.md`
- `docs/decisions.md`
- `result.md`
- `scripts/chaos.sh`.

### Требования
- Повторить F1, F5, F6 на актуальном коде.
- F5 в fault-injection сейчас говорит про зависание GET >15s, но result.md описывает уже внедрённый TimeLimiter/CircuitBreaker и быстрый 503. Указать только актуальный проверенный результат.
- Для F1 повторно проверить безопасность Nginx retry POST после исправления idempotency.
- Для каждого сценария записать нагрузку, длительность, команды, точные HTTP errors, p95/p99, lag, drain time, потери/дубли.
- Не писать «клиент ничего не заметил», если были ошибки/рост latency.
- Если тест не запускался повторно, отметить его как исторический результат, а не текущую проверку.

---

# P1 — важные улучшения

## P1-1. История инцидентов

Проверить бизнес-модель `metric.incidents`. Сейчас `IncidentRowRepository.open()` использует `ON CONFLICT (incident_key) DO UPDATE`, а ключ `userId:ruleId` постоянен. Повторное открытие после закрытия перезаписывает ту же строку. Это может уничтожать историю предыдущих инцидентов.

Согласовать с владельцем модель:
- один объект на постоянный lifecycle rule (тогда это скорее текущее состояние, не история);
- или отдельная запись на каждый occurrence (предпочтительно для истории инцидентов).

Если нужна история, ввести occurrence/incident ID и уникальность на transition/open event. Добавить тест: открыть → закрыть → открыть снова; GET history должен вернуть два отдельных периода.

## P1-2. Consumer lag в Prometheus/Grafana

Добавить Kafka lag exporter или эквивалентный способ экспорта lag по:
- topic;
- consumer group;
- partition.

Обязательные группы:
- `storage-group` на `metrics`;
- `processing-group` на `metrics`;
- `notification-group` на `incidents`;
- `metric-group` на `incidents`.

Добавить Grafana panels:
- current lag;
- lag growth rate;
- consumer throughput;
- time-to-drain после восстановления.
Проверить, что exporter действительно видит все группы и не создаёт высокую cardinality.

## P1-3. End-to-end latency

Добавить измерения:
- ingest accepted → ClickHouse query-visible;
- ingest accepted → incident published;
- incident published → notification SENT;
- Kafka event age на входе каждого consumer.

Использовать bounded labels (не userId, eventId, metric name из произвольного ввода). Добавить histogram/summary и панели p50/p95/p99. Проверить требования 5s/10s/20s на нагрузке, а не выводить их из HTTP latency.

## P1-4. Проверка масштабирования

С текущими двумя partitions в каждой consumer group максимум два активных consumer instances. Провести эксперимент 1→2→3 replicas и показать, что третья реплика не добавляет parallelism при 2 partitions. Это не дефект, если так задумано, но должно быть явно описано.

Если нужно больше параллелизма — представить владельцу предложение по числу partitions и ordering semantics. Не увеличивать partitions молча: ключ userId и порядок обработки точек должны сохраняться.

## P1-5. Проверить ClickHouse idempotency и partial failures

Добавить интеграционные тесты:
- повторное получение того же MetricEvent;
- batch insert с частичной/сетевой ошибкой;
- повтор после неопределённого результата insert;
- SELECT с FINAL возвращает ожидаемое количество уникальных точек;
- timestamp/eventId sorting key корректен.

Не заявлять exactly-once. Описывать гарантию как at-least-once + дедупликация по event identity на уровне ClickHouse/read semantics, если тесты это подтверждают.

## P1-6. Notification delivery semantics

Документировать, что `FAILED` сейчас терминален: повтор Kafka event не инициирует новую доставку, потому что source_event_id уже claimed. Добавить:
- метрику delivery duration;
- счётчики attempts/SENT/FAILED;
- возраст oldest pending/DELIVERING;
- механизм ручного или контролируемого retry FAILED, если он требуется;
- тест на рестарт во время доставки и duplicate incident event.

Синхронный HTTP delivery и sleep в Kafka listener оставить в DEMO только с явным ограничением. Не превращать listener в бесконечно блокирующий consumer.

## P1-7. Nginx / gateway ограничения

- Описать один Nginx как SPOF DEMO.
- Проверить фактическое распределение upstream по логам при 2+ metric-service.
- Проверить graceful scale-down и keep-alive connections.
- `proxy_next_upstream non_idempotent` разрешать только после корректного P0-2.
- Не называть DNS discovery readiness/health checks: DNS возвращает адреса контейнеров, но не обязательно проверяет готовность приложения.

## P1-8. Защита портов Compose

Для сценария размещения на VPS оставить внешне опубликованным только необходимое:
- Nginx :80/:443;
- Grafana/Prometheus — только если нужны извне и с ограничением доступа.
Postgres, ClickHouse, Kafka и Redis должны оставаться внутри Docker network, если нет конкретной причины публиковать их на host. Не менять dev workflow без документирования.

## P1-9. Retention ClickHouse

Оценить объём при заявленной интенсивности:
- 20k points/s ≈ 1.728 млрд точек/сутки;
- 60k points/s ≈ 5.184 млрд точек/сутки.

Согласовать retention/TTL и политику удаления/архивации. Не включать TTL без согласования, потому что это меняет пользовательскую историю. Добавить оценку дискового объёма и предупреждение о storage growth.

## P1-10. Пропускная способность отправителя ingest-outbox (находка замера P0-3)

Замер 2026-11-08 (`docs/results-et7.md`): HTTP-приём держит 60k точек/с, но
отправитель outbox публикует в Kafka ≈ 42 события/с (≈ 4.3k точек/с при
batch=100) — каждое событие синхронно ждёт `ack`. сквозной end-to-end ≈ 4.5k
точек/с, при нагрузке выше этого растёт `pulse.ingest.outbox.pending` (в прогоне
до ~55 000 событий, дренаж 25 мин).

Сделать конвейерную публикацию порции: `send()` всех событий батча без ожидания,
затем ожидание всех `Future` (или `callback` + групповое закрытие порции
одной транзакцией). Гарантию at-least-once не менять (повтор байт-в-байт,
eventId из очереди). Метрики глубины/возраста уже есть; добавить гистограмму
времени порции. Нагрузочно: повторить step 20/40/60k — целевой сквозной drain
без часового отставания на этом стенде; целевое число согласовать с владельцем.

---

# P2 — улучшения после обязательных работ

1. Контролируемый graceful drain при scale-down.
2. Отдельный сценарий нагрузки с массовыми нарушениями правил.
3. Ограниченный worker pool для уведомлений.
4. Профили Compose: минимальный demo, observability, при необходимости multi-broker.
5. Уточнить API security: исходное ТЗ упоминает API key, текущий код использует Basic auth. Не менять контракт без решения владельца; вынести расхождение на согласование.
6. Добавить p99 и HTTP status breakdown в отчёты.
7. Проверить Prometheus labels на cardinality.

---

# Итоговые deliverables после исправлений

Агент должен обновить:
- исходный код и миграции;
- integration tests для всех исправленных failure windows;
- `loadgen.py`;
- `docs/results-et7.md` с новыми реальными замерами;
- `docs/fault-injection.md` с повторными сценариями;
- `result.md` с актуальными возможностями и ограничениями;
- README, если команды/контракты изменились.

В финальном отчёте перечислить:
1. что исправлено (файлы и краткое описание);
2. какие тесты запускались и точные результаты;
3. какая нагрузка достигнута в HTTP RPS и points/s;
4. p50/p95/p99 и ошибки;
5. consumer lag и время drain;
6. результаты fault injection;
7. что осталось ограничением и почему.

**Критерий готовности:** проект не просто компилируется. Он должен корректно переживать повторную доставку и сбои в критических окнах, демонстрировать масштабирование в пределах числа partitions, измерять фактическую пропускную способность и честно показывать пределы single-host DEMO.
