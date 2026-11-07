package com.pulse.metric.repository;

import com.pulse.contracts.dto.MetricReading;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * История метрик (Этап 5, FR-4): читает pulse.metric_points из ClickHouse,
 * дедуплицируя повторную доставку Kafka через FINAL (ENGINE = ReplacingMergeTree).
 */
@Repository
public class MetricHistoryRepository {

    // ClickHouse JDBC (SqlBasedPreparedStatement) подставляет параметры как текст,
    // поэтому границы времени передаются литералом parseDateTimeBestEffort('ISO-8601')
    // — значение форматируется нами из Instant, инъекция исключена.
    private static final String SQL_TEMPLATE =
            "SELECT metricName, value, timestamp FROM pulse.metric_points FINAL"
                    + " WHERE userId = ?"
                    + " AND timestamp >= parseDateTimeBestEffort('%s')"
                    + " AND timestamp <= parseDateTimeBestEffort('%s')%s"
                    + " ORDER BY timestamp DESC LIMIT ?";

    private static final String METRIC_FILTER = " AND metricName = ?";

    private final JdbcTemplate clickhouseTemplate;

    public MetricHistoryRepository(@Qualifier("clickhouseJdbcTemplate") JdbcTemplate clickhouseTemplate) {
        this.clickhouseTemplate = clickhouseTemplate;
    }

    /**
     * Точки метрик пользователя за период [from, to], новые сначала.
     * metricName — optional-фильтр (null = все метрики).
     */
    public List<MetricReading> find(UUID userId, String metricName, Instant from, Instant to,
                                    int limit) {
        boolean filterByMetric = metricName != null && !metricName.isBlank();
        String sql = String.format(SQL_TEMPLATE, from.toString(), to.toString(),
                filterByMetric ? METRIC_FILTER : "");
        Object[] args = filterByMetric
                ? new Object[]{userId.toString(), metricName, limit}
                : new Object[]{userId.toString(), limit};
        return clickhouseTemplate.query(sql,
                (resultSet, rowNumber) -> new MetricReading(
                        resultSet.getString(1),
                        resultSet.getDouble(2),
                        resultSet.getTimestamp(3).toInstant()),
                args);
    }
}
