# Fault injection (Этап 8)

Стенд: compose-кластер (см. README «Целевая система»), нагрузка `loadgen.py`
baseline/20 rps, фоновые проверки HTTP-кодов. Воспроизведение сценариев —
`scripts/chaos.sh` (шаги приведены там же).

| # | Сценарий | Клиентский эффект | Данные | Восстановление |
|---|---|---|---|---|
| F1 | `docker kill -9` реплики metric-service под нагрузкой (nginx retry `non_idempotent` — по находке Этапа 7) | **0 ошибок** из 799, только пик единичного соединения 404 мс | нет потерь | реплика поднимается `compose up -d`, nginx сам видит A-запись |
| F2 | остановка storage-service | 0 ошибок (ingest идёт в Kafka), p95 6 мс | LAG `storage-group` ≈ 800 | после start: LAG→0 за ~23 c, no-loss (batch ack-mode: commit только после записи CH) |
| F3 | остановка processing-service | 0 ошибок, p95 6 мс | LAG `processing-metrics` ≈ 800; storage-LAG=0 (пути независимы) | LAG→0 за ~23 c |
| F4 | остановка notification-service | ingest 202 в штатном режиме | LAG `notification-group` растёт; инциденты уже в PG/топике | после start инциденты доставлены, LAG→0 ~25 c |
| F5 | остановка ClickHouse | ingest 202 (не зависит от CH); **read API деградирует: ответ не возвращается >15 с** (см. «Находки») | LAG storage 15→0 за 26 c, no-loss | read восстанавливается сразу после старта CH |
| F6 | `docker kill -9` брокера kafka1 (RF2, min.insync=2) | ingest **503** на время отказа (`NOT_ENOUGH_REPLICAS`); перед и после — 202 | продюсер c `enable.idempotence` + неограниченными retry **дозаписал** всё в брокер после возврата (ошибки клиента ≠ потеря данных — это at-least-once на записи) | после `up -d kafka1` всё зелено; это ожидаемый предел конфигурации RF2/min.insync=2 |
| F7 | остановка PostgreSQL | auth/read/ingest деградируют (PG — пользователи, идемпотентность, инциденты) | Kafka и ClickHouse продолжают работать, новые read/ingest после восстановления — 200/202 | после healthy PG всё восстанавливается без рестарта сервисов |

## Находки (кандидаты в Этап 9 / бэклог)

1. **Read-путь не fail-fast.** При мёртвой зависимости GET запрос висит до
   клиентского таймаута (в замерах ≥15 с) вместо отказа в ~2–3 c: пул HikariCP
   переиспользует idle-соединение, а validation у ClickHouse/PostgreSQL driver
   обрывается только со켓-таймаутом. Частично закрыто: JDBC `connectTimeout/socketTimeout`
   + `hikari.connection-timeout=3s` добавлены во все сервисы. Полное решение —
   resilience4j (circuit breaker + bulkhead) на read-пути.
2. **503 у ingest ≠ потеря**: семантика at-least-once (идемпотентный продюсер +
   retry) проверена на F6 — важно для SLA клиента и будущего front-offфиса повторов.
3. Kafka RF2/`min.insync.replicas=2` = аптайм при отказе одного брокера, но
   отказ любой из пар реплик топика `metrics` останавливает запись (F6). Для
   прода — RF≥3/`acks=all`/`min.insync=2` и отдельные зоны.
4. `proxy_next_upstream ... non_idempotent` (nginx) закрыл разрывы keep-alive при
   scale-down — ошибка Этапа 7 (32/3157) на F1 не воспроизвелась (0 ошибок).
