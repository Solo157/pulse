package com.pulse.processing.state;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Доступ к состоянию инцидентов. Все переходы — одиночные атомарные операторы:
 * порядок точек одного пользователя гарантирует key сообщения Kafka, поэтому
 * гонок между экземплярами ProcessingService по одному ключу нет
 * (одна партиция на key, consumer в группе назначен единственный раз).
 */
@Repository
public class IncidentStateRepository {

    private final JdbcTemplate jdbcTemplate;

    public IncidentStateRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Регистрирует факт «точка с временем {@code sampleAt} осмотрена» по ключу инцидента.
     * Возвращает актуальное состояние, если точка свежее уже виденных, и пустой результат
     * для out-of-order точки (строго {@code >}: повтор доставки того же времени тоже устарел).
     */
    public Optional<IncidentState> registerSample(String incidentKey, UUID userId, String ruleId,
            String metricName, Instant sampleAt) {
        return jdbcTemplate.query("""
                INSERT INTO metric.incident_state
                    (incident_key, user_id, rule_id, metric_name, open, last_violation_at, last_sample_at)
                VALUES (?, ?, ?, ?, FALSE, NULL, ?)
                ON CONFLICT (incident_key) DO UPDATE
                    SET last_sample_at = EXCLUDED.last_sample_at
                WHERE metric.incident_state.last_sample_at IS NULL
                   OR metric.incident_state.last_sample_at < EXCLUDED.last_sample_at
                RETURNING open, last_violation_at, last_sample_at
                """, (rs, rowNum) -> mapState(rs, userId, ruleId, metricName),
                incidentKey, userId, ruleId, metricName, Timestamp.from(sampleAt))
                .stream().findFirst();
    }

    /**
     * Открывает инцидент: фиксировалась до этого точка нарушения в момент {@code violationAt}.
     * Условие {@code open = FALSE} гарантирует один переход и один event на период;
     * возвращает {@code false}, если инцидент уже был открыт (защита от гонок и повторов).
     */
    public boolean openIncident(String incidentKey, Instant openedAt, Instant violationAt) {
        return jdbcTemplate.update("""
                UPDATE metric.incident_state
                SET open = TRUE, opened_at = ?, last_violation_at = ?
                WHERE incident_key = ? AND open = FALSE
                """, Timestamp.from(openedAt), Timestamp.from(violationAt), incidentKey) == 1;
    }

    /** Продлевает непрерывное нарушение (инцидент уже открыт). */
    public void registerViolation(String incidentKey, Instant violationAt) {
        jdbcTemplate.update("UPDATE metric.incident_state SET last_violation_at = ? WHERE incident_key = ?",
                Timestamp.from(violationAt), incidentKey);
    }

    /**
     * Закрывает инцидент (значение вернулось в норму не раньше cooldown).
     * Условие {@code open = TRUE} — один переход и один event; {@code false}, если уже закрыт.
     */
    public boolean closeIncident(String incidentKey) {
        return jdbcTemplate.update(
                "UPDATE metric.incident_state SET open = FALSE WHERE incident_key = ? AND open = TRUE",
                incidentKey) == 1;
    }

    private IncidentState mapState(ResultSet rs, UUID userId, String ruleId, String metricName)
            throws SQLException {
        return new IncidentState(userId, ruleId, metricName, rs.getBoolean("open"),
                toInstant(rs.getTimestamp("last_violation_at")),
                Objects.requireNonNull(toInstant(rs.getTimestamp("last_sample_at"))));
    }

    private Instant toInstant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }
}
