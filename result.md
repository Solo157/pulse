# Итоговый отчёт (result.md)

Проект: **Pulse** — отказоустойчивый конвейер телеметрии (learning-проект по
нагрузке/скейлу/fault injection). Стенд поднимается `docker compose up -d`
(креды `.env`), тесты `mvn clean install` (нужен Docker).

## Что построено (по этапам `implementation.md`)

| Этап | Результат |
|---|---|
| 0–1 Анализ/скелет | требования, архитектура (README «Целевая система»), compose-стенд, контракты событий (schemaVersion) |
| 2 Приём/хранение | `metric-service` (идемпотентный ingest, Basic-auth), `storage-service` (batch→ClickHouse, dedup по eventId), end-to-end тесты |
| 3 Обработка/инциденты | `processing-service`: правила/пороги → инциденты PostgreSQL + событие `incidents`, key=userId, порядок в партиции |
| 4 Уведомления | `notification-service`: доставка (webhook-заглушка), статусы SENT/FAILED, идемпотентность, ретраи |
| 5 Read API | история метрик/инцидентов владельцу (ClickHouse+PostgreSQL), Basic-аутентификация, 401/403 |
| 6 Observability | Actuator/Micrometer на 4 сервисах, Prometheus dns_sd (порты 8080–8083), Grafana-дашборд «Pulse» (бизнес + системные SLI) |
| 7 Нагрузка/scale | `loadgen.py` (baseline/step/burst), замеры `docs/results-et7.md`, scale-под-нагрузкой |
| 8 Fault injection | 7 сценариев F1–F7 `docs/fault-injection.md`, шаги `scripts/chaos.sh` |

## Качество

- Модульные + интеграционные тесты (Testcontainers): **29/29 зелёные**, `mvn clean install` — BUILD SUCCESS.
- Код-стайл по договорённостям БЗ (конспекты кода, Javadoc, конвенции имён/слоёв).
- CI-гейты гейт-скиллов (build/tests/openspec-N/A/lint-N/A/config/tests).

## Нагрузка (см. `docs/results-et7.md`)

baseline 10 rps: 600/0 ошибок, p95 10 мс; step 10→20→40 rps: 4190/0, p95 ≈ 7 мс
(плоская латентность при ×4); burst 50 rps: 0 ошибок. Scale up→down под пиком:
обрыв keep-alive на SIGTERM реплики, 32/3157 ошибок (~1 %), LAG=0, **ноль потерь**
(3157 принято = 3157 строк ClickHouse). Обнаружен и закрыт nginx-респонсом
`proxy_next_upstream non_idempotent` (повтор POST по Idempotency-Key).

## Отказы (см. `docs/fault-injection.md`)

kill реплики metric — 0 ошибок; остановка storage/processing/notification —
клиент без деградации, LAG ~800 дренаж ~23 с, no-loss (batch ack-mode);
остановка ClickHouse — ingest жив, read деградирует (hang, см. находку);
остановка PostgreSQL — auth/ingest/read деградируют (PG = ядро состояния);
kill брокера Kafka (RF2, min.insync=2) — 503 `NOT_ENOUGH_REPLICAS`, но
идемпотентный продюсер дозаписывает: 503 клиенту ≠ потеря данных.

## Находки / бэклог развития

1. ~~Read-путь не fail-fast~~ — **закрыто**: resilience4j TimeLimiter +
   CircuitBreaker (`chRead`/`pgRead`) в `UserReadService`, изолированный пул выборок;
   ответ 503 за ~1 с при мёртвом хранилище метрик, ~100 мс при открытой цепи
   (`docs/decisions.md`, `ReadFailFastIntegrationTest`).
2. Kafka-пароли consumer-групп = потолок параллелизма (партиций `metrics`=2):
   для прода — больше партиций + RF≥3.
3. нет экспортёра Kafka-LAG в Prometheus (читается CLI); добавить kafka-exporter.
4. Redis под быстрый слой идемпотентности развёрнут, но не задействован в коде.
5. 503 при отказе брокера стоит транслировать клиенту как retry-after (сейчас
   503 в общем стиле).
6. nginx graceful drain соединений при scale-down (сейчас спасает retry).

## Runbook

```bash
docker compose --profile obs up -d         # полный стенд (приложение + мониторинг)
curl -X POST localhost/auth/register -H 'Content-Type: application/json' \
     -d '{"email":"u@pulse.dev","password":"Passw0rd!!"}'                  # -> userId
curl -X POST localhost/users/<userId>/metrics -H "Authorization: Basic $(echo -n <userId>:Passw0rd!! | base64)" \
     -H 'Idempotency-Key: demo-1' -H 'Content-Type: application/json' \
     -d '{"metrics":[{"name":"temperature","value":91.0,"timestamp":"2026-11-07T09:01:00Z"}]}'  # -> 202
curl "localhost/users/<userId>/metrics" -u "<userId>:Passw0rd!!"            # история
curl "localhost/users/<userId>/incidents" -u "<userId>:Passw0rd!!"          # инциденты -> OPEN + notification SENT

./loadgen.py                                  # baseline-профиль (env LOAD_*)
./scripts/chaos.sh                            # воспроизведение сценариев отказа (шаги в файле)
docker compose down -v                        # полный сброс (чистые тома Kafka/PG/CH)
mvn clean install                             # тесты
```

Ограничения стенда: единый хост 16 CPU (генератор и система разделяют CPU);
`min.insync.replicas=2` при RF2 — компромисс latency/доступности для учебной
мишени. Абсолютные значения латентности не переносятся на чистый прод-стенд.
