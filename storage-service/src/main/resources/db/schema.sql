-- История метрик (раздел 6.8): пишет её storage-service, читает read API
-- metric-service. Kafka умеет отдать то же событие повторно, поэтому обработка
-- повторов — «вариант B» (согласование п. 8): идентификатор события eventId
-- входит в ключ сортировки (ORDER BY), а движок таблицы ReplacingMergeTree
-- сам убирает одинаковые строки при слиянии данных в фоне. Чтобы и до слияния
-- читалось по одному разу, в SELECT добавляют ключевое слово FINAL.
-- Файл безопасно выполнять много раз: он запускается и при старте контейнера
-- ClickHouse, и при старте сервиса.

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
