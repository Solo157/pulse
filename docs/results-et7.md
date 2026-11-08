# Результаты нагрузочных испытаний (Этап 7)

Методика. Генератор `loadgen.py` (stdlib, планировщик RPS по окнам, стабильный
Idempotency-Key на пакет) бьёт `POST /users/{id}/metrics` через nginx. Профили:
`baseline` (плоский RPS), `step` (ступени), `burst` (пик). Метрики стека —
Prometheus (`pulse_*` счётчики), проверка отсутствия потерь — COUNT по ClickHouse,
лаг — `kafka-consumer-groups.sh` (LAG по группам `storage-group`, `processing-group`).
Стенд: один хост (16 CPU / 31 ГБ), все контейнерыcompose вместе с  генератором,
Kafka 3 брокера / 2 партиции топика `metrics`, PostgreSQL/ClickHouse без лимитов.

## Замеры (2026-11-07)

| Профиль | Нагрузка | Отправлено | Ошибок | p50, мс | p95, мс | max, мс | Итог |
|---|---|---:|---:|---:|---:|---:|---|
| baseline | 10 rps, 60 c | 600 | 0 | 7,97 | 10,09 | 14,29 | все этапы обработаны |
| step | 10→20→40 rps ×60 c | 4190 | 0 | 5,09 | 7,06 | 15,12 | латентность плоская при росте ×4 |
| burst | 10 rps + пик 50 rps (20 c) | 1396 | 0 | 4,35 | 5,82 | 7,94 | пик переварен без деградации |
| scale-под-нагрузкой | burst 50 rps, scale up→down | 3157 | 32 (≈1 %) | 5,24 | 18,82 | 1323 | см. ниже |
| scale (up) | metric=2, storage=2, processing=2 | — | 0 | — | — | — | Prometheus-таргеты 2/2/2 UP, dns_sd без рестарта |
| scale (down во время пика) | обратно в 1 реплику | — | 32 | — | до 1,3 c | — | LAG=0 после окончания; **потерь нет** |

Потери при scale-down: ни одной. Принято генератором 3157 (ошибки 32 не засчитаны
как принятые), в `pulse.metric_points` ровно **3157** строк; LAG обеих consumer-групп = 0.
Природа 32 ошибок: обрыв TCP к реплике metric-service, которой шёл SIGTERM при scale-down
(стоп-сигнал раньше завершения keep-alive-соединений nginx; graceful-shutdown на 10 c
не успевает дренаж keep-alive-соединений nginx) — кандидат на улучшение в Этап 8/9.

Повтор: `LOAD_EMAIL=<уникальный> LOAD_PASSWORD=<8+> LOAD_PROFILE=baseline LOAD_RPS=10
./loadgen.py` (профили `step`: `LOAD_STEP_MULTS=1,2,4 LOAD_DURATION=180`; `burst`:
`LOAD_BURST_MULT=5 LOAD_BURST_SECONDS=20`). Масштабирование под нагрузкой:
`docker compose up -d --scale metric-service=2 --scale storage-service=2
--scale processing-service=2` и обратно в 1.

Ограничения: единый хост (и генератор, и система — разделяют CPU; абсолютные
значения латентности не переносятся на чистый стенд); экспортёр лагов Kafka как
метрика Prometheus не развёрнут — LAG читался из CLI consumer-groups; Idempotency-Key
в профиле всегда уникален (идемпотентные повторы покрываются интеграционными тестами).
