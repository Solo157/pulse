package com.pulse.processing.state;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Запись инцидентов в {@code metric.incidents} (таблицу создаёт MetricService).
 * Одна строка на бизнес-ключ {@code incident_key}: переход открыт→закрыт обновляет
 * status/opened_at/closed_at. Операторы идемпотентны к повторной доставке
 * {@code IncidentDetected}: повторный open не оставляет второй строки, повторный
 * close не трогает уже закрытый инцидент.
 */
@Repository
public class IncidentRowRepository {

    private static final String STATUS_OPEN = "OPEN";
    private static final String STATUS_CLOSED = "CLOSED";

    private final JdbcTemplate jdbcTemplate;

    public IncidentRowRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /** Событие открытия: новая строка или повторное открытие существовавшего ключа. */
    public void open(UUID eventId, String incidentKey, UUID userId, String ruleId, String metricName,
            Double value, Double threshold, Instant openedAt) {
        jdbcTemplate.update("""
                INSERT INTO metric.incidents
                    (id, user_id, rule_id, metric_name, incident_key, status, value, threshold,
                     opened_at, closed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                ON CONFLICT (incident_key) DO UPDATE
                SET status = EXCLUDED.status,
                    value = EXCLUDED.value,
                    threshold = EXCLUDED.threshold,
                    opened_at = EXCLUDED.opened_at,
                    closed_at = NULL,
                    id = EXCLUDED.id
                """, eventId, userId, ruleId, metricName, incidentKey, STATUS_OPEN,
                value, threshold, Timestamp.from(openedAt));
    }

    /** Событие закрытия: только для ещё открытой строки; повторного закрытия не бывает. */
    public void close(String incidentKey, Instant closedAt) {
        jdbcTemplate.update("""
                UPDATE metric.incidents
                SET status = ?, closed_at = ?
                WHERE incident_key = ? AND status = ?
                """, STATUS_CLOSED, Timestamp.from(closedAt), incidentKey, STATUS_OPEN);
    }
}
