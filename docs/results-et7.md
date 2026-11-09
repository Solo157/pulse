# Результаты нагрузочных испытаний

## Методика

Генератор `loadgen.py` измеряет интенсивность в **точках метрик**: payload
содержит `LOAD_BATCH_SIZE` точек, целевая интенсивность задаётся
`LOAD_TARGET_POINTS_PER_SEC` и однозначно даёт HTTP RPS = target/batch.
В отчёте раздельны offered load (planned), принятая отдача (accepted по ответам
202) и подтверждённая персистентность (сверка COUNT по ClickHouse + LAG Kafka).
Очередь работ ограничена, переполнение считается (`dropped_jobs`), молча не
теряется ни одна задача; каждая проверочная команда обязана завершиться успешно
(иначе — ошибка в отчёте, а не «нуль»).

Профили: `baseline` (плоский темп), `step` (ступени), `burst` (фон + пик).

Лаг Kafka читается `kafka-consumer-groups.sh` **в единицах сообщений**: при
batch=100 одно сообщение = 100 точек. Корректная команда для стенда:
`docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server
localhost:9092 --describe --all-groups` (скрипт лежит в `/opt/kafka/bin`, в PATH
его нет — «пустая» команда с конвейером `| awk` иначе показывает LAG=0).

Образы сервисов собираются из локальных `*/target/*.jar`: перед
`docker compose build` обязателен `mvn package -DskipTests`, иначе в контейнер
попадает старый код.

Стенд: один хост (16 CPU / 31 ГБ), генератор рядом со стеком, Kafka 3 узла
(два брокера + controller-only), ClickHouse/PostgreSQL без лимитов.

## Шаг 20k→40k→60k точек/с (2026-11-08)

batch=100, 32 потока отправки, ступени по 60 с, без нарушений правил.

| Показатель | Значение |
|---|---|
| Отправлено / принято | 7 200 000 / **7 200 000** точек (ошибок 0, retry 0, dropped 0) |
| Интенсивность приёма (HTTP, 202 = запись в PostgreSQL-outbox) | все ступени выдержаны wall=60.0 с, на пике — **60 000 точек/с** |
| Латентность HTTP (p50/p95/p99/max), мс | 3.0 / 62.0 / 92.6 / 138.2 |
| Очередь генератора high-water | 368 из 400 |
| ClickHouse, финальный COUNT | **7 200 000 — ровно как принято**: потерь и необъяснимых дублей нет |
| Устойчивая доставка в Kafka (отправитель outbox) | **≈ 42 события/с ≈ 4 300 точек/с**: каждое событие подтверждается синхронным ожиданием `ack`; за прогон в очередях накапливалось до ~55 000 событий |
| Сквозной end-to-end до ClickHouse | **≈ 4 300–4 500 точек/с**; все точки в CH через 1 530 с после конца нагрузки; LAG всех групп 0 |

## Итог

Пропускная способность стенда на этом профиле:

- **приём** (HTTP → журнал идемпотентности + outbox в PostgreSQL): 60 000
  точек/с без ошибок;
- **дальше конвейер сужается**: доставщик outbox метрик публикует в Kafka
  ≈ 4.3k событий-точек/с (синхронный per-event `ack`), хранилище успевает за
  ним в реальном времени;
- сквозная гарантия «все принятые точки в ClickHouse» выдержана без потерь и
  дублей; чтобы держать 20k/60k точек/с **сквозь**, доставщику нужна
  конвейерная пакетная публикация (групповое ожидание ack вместо per-event).

Числа 20k/60k корректно заявлять только для участка приёма.

## Как повторить

```bash
LOAD_PROFILE=step LOAD_BATCH_SIZE=100 LOAD_TARGET_POINTS_PER_SEC=20000 \
LOAD_STEP_MULTS=1,2,3 LOAD_STEP_SECONDS=60 LOAD_USERS=32 \
LOAD_EMAIL=step-$(date +%s)@pulse.dev LOAD_PASSWORD=<8+> \
LOAD_CH_COUNT_CMD="docker exec clickhouse clickhouse-client --query \"SELECT count() FROM pulse.metric_points WHERE userId='{user}'\"" \
LOAD_KAFKA_LAG_CMD='docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups 2>/dev/null | awk "NR>1 && NF>=6 && \$6!=\"-\" {lag+=\$6} END {print lag+0}"' \
python3 loadgen.py
```

Параметры: `LOAD_BATCH_SIZE` (1..100), `LOAD_TARGET_POINTS_PER_SEC` | `LOAD_RPS`,
`LOAD_DURATION`, `LOAD_USERS`, `LOAD_PROFILE=baseline|step|burst`,
`LOAD_STEP_MULTS`, `LOAD_BURST_MULT/SECONDS`, `LOAD_VIOLATION_SHARE` (доля точек
с превышением порога правил), `LOAD_HTTP_RETRIES`, `LOAD_DRAIN_TIMEOUT`,
`LOAD_CH_COUNT_CMD`/`LOAD_KAFKA_LAG_CMD` (плейсхолдер `{user}`), `LOAD_OUT`.

## Ограничения

Единый хост: генератор и система делят CPU, абсолютная латентность не
переносится на чистый стенд. `LOAD_DRAIN_TIMEOUT` по умолчанию 300 с — при
нагрузке выше пропускной способности доставщиков дренаж не успевает
(фиксируется `drain_complete=false`; итоговый COUNT — отдельным замером после
дренажа). Профиль step без `LOAD_VIOLATION_SHARE` не проверяет обработку
инцидентов под нагрузкой — это отдельный прогон.
