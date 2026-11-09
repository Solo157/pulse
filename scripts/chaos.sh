#!/usr/bin/env bash
# Этап 8 — fault injection: воспроизводимые сценарии (запуск из корня репозитория).
# Каждый сценарий = несколько shell-шагов; результаты фиксируются в docs/fault-injection.md.
# Требуется поднятый docker compose + scripts/../loadgen.py.
# ВАЖНО: образы копствуют jar из локальных */target/ — перед прогоном
# пересобрать: mvn package -DskipTests && docker compose build <сервисы>.
#
#   F1 metric-kill      : docker compose up -d --scale metric-service=2
#                         (нагрузка ./loadgen.py baseline 20rps) ->
#                         docker kill --signal=KILL $(docker compose ps -q metric-service | tail -1)
#   F2 storage-stop     : docker compose stop storage-service  (LAG storage-group растёт)
#                         docker compose start storage-service (LAG -> 0)
#   F3 processing-stop  : аналогично, --group processing-group
#   F4 notification-stop: docker compose stop notification-service; шлём метрики > порога;
#                         LAG notification-group растёт -> start -> инциденты/SENT в PG
#   F5 clickhouse-stop  : GET /metrics -> 503 за <=3 c (TimeLimiter/CircuitBreaker),
#                         ingest 202, LAG storage-group -> дренаж после старта
#   F6 kafka-kill       : docker kill --signal=KILL kafka1 -> клиент продолжает
#                         получать 202 (приём в outbox PG не зависит от Kafka);
#                         после старта брокера outbox дозапускается за секунды
#   F7 postgres-stop    : auth/read/ingest деградируют (PG = auth/идемпотентность/инциденты)
#
# LAG: docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
#        --bootstrap-server localhost:9092 --describe --group <group>
