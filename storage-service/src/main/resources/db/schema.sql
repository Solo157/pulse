-- Схема ClickHouse для MetricService-потока (раздел 6.8).
-- Дедупликация повторной доставки Kafka ("вариант B", согласование п. 8):
-- eventId входит в ORDER BY, повторная вставка тех же точек схлопывается движком
-- ReplacingMergeTree; читается через FINAL (+ очистка OPTIMIZE позже).
-- Идемпотентно: выполняется и при старте контейнера ClickHouse, и при старте сервиса.

CREATE DATABASE IF NOT EXISTS pulse;

CREATE TABLE IF NOT EXISTS pulse.metric_points (
    userId       UUID,
    metricName   String,
    value        Float64,
    timestamp    DateTime64(3, 'UTC'),
    eventId      UUID,
    insertedAt   DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(insertedAt)
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (userId, metricName, timestamp, eventId);
