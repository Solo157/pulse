package com.pulse.metric.repository;

import com.pulse.contracts.dto.IncidentResponse;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Инциденты пользователя для read API (FR-4, Этап 5) — таблица metric.incidents,
 * которую ведёт ProcessingService; MetricService читает её только для отдачи истории.
 */
@Repository
public class IncidentReadRepository {

    private final JdbcTemplate jdbcTemplate;

    public IncidentReadRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Инциденты пользователя за период (opened_at >= from), новые сначала.
     */
    public List<IncidentResponse> find(UUID userId, Instant from, int limit) {
        return jdbcTemplate.query(
                "SELECT user_id, rule_id, metric_name, status, value, threshold,"
                        + " opened_at, closed_at FROM metric.incidents"
                        + " WHERE user_id = ? AND opened_at >= ? ORDER BY opened_at DESC LIMIT ?",
                IncidentReadRepository::mapRow, userId, Timestamp.from(from), limit);
    }

    private static IncidentResponse mapRow(ResultSet resultSet, int rowNumber) throws SQLException {
        Double value = resultSet.getObject(5, Double.class);
        Double threshold = resultSet.getObject(6, Double.class);
        Timestamp closedAt = resultSet.getTimestamp(8);
        return new IncidentResponse(
                resultSet.getObject(1, UUID.class).toString(),
                resultSet.getString(2),
                resultSet.getString(3),
                resultSet.getString(4),
                value,
                threshold,
                resultSet.getTimestamp(7).toInstant(),
                closedAt == null ? null : closedAt.toInstant());
    }
}
