package com.pulse.storage.service;

import com.pulse.contracts.common.MetricPoint;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * Пакетная запись точек в ClickHouse (раздел 6.3). Повторная доставка того же
 * события Kafka безопасна: идентификатор события входит в ключ сортировки,
 * и движок таблицы сам убирает одинаковые строки (согласование п. 8).
 */
@Service
public class ClickHousePointsWriter {

    private static final Logger log = LoggerFactory.getLogger(ClickHousePointsWriter.class);
    private static final String INSERT_SQL = """
            INSERT INTO pulse.metric_points (userId, metricName, value, timestamp, eventId)
            VALUES (?, ?, ?, ?, ?)
            """;

    private final JdbcTemplate jdbcTemplate;
    private final Counter insertedPoints;
    private final Counter insertFailures;

    public ClickHousePointsWriter(JdbcTemplate jdbcTemplate, MeterRegistry meterRegistry) {
        this.jdbcTemplate = jdbcTemplate;
        this.insertedPoints = meterRegistry.counter("pulse.storage.inserted_points");
        this.insertFailures = meterRegistry.counter("pulse.storage.insert_failures");
    }

    /** Вставляет все точки одного MetricEvent одним пакетом. */
    public void insertPoints(String userId, String eventId, List<MetricPoint> points) {
        List<Object[]> rows = new ArrayList<>(points.size());
        for (MetricPoint point : points) {
            rows.add(new Object[]{
                    userId,
                    point.name(),
                    point.value(),
                    Timestamp.from(point.timestamp()),
                    eventId});
        }
        try {
            jdbcTemplate.batchUpdate(INSERT_SQL, rows);
            insertedPoints.increment(rows.size());
            log.debug("Записано {} точек (событие {})", rows.size(), eventId);
        } catch (RuntimeException exception) {
            insertFailures.increment();
            throw exception;
        }
    }
}
