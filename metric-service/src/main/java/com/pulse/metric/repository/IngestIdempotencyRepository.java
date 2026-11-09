package com.pulse.metric.repository;

import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Журнал идемпотентности ingest (таблица metric.ingest_idempotency).
 * Один шаг "приём" на пару (ключ, пользователь).
 */
@Repository
public class IngestIdempotencyRepository {

    public static final String STATUS_IN_PROGRESS = "IN_PROGRESS";
    public static final String STATUS_COMPLETED = "COMPLETED";

    private final JdbcTemplate jdbcTemplate;

    public IngestIdempotencyRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /** Пытается создать запись со статусом IN_PROGRESS. true — если ключа ещё не было. */
    public boolean tryCreate(String idempotencyKey, UUID userId) {
        int inserted = jdbcTemplate.update("""
                INSERT INTO metric.ingest_idempotency (idempotency_key, user_id, status)
                VALUES (?, ?, ?)
                ON CONFLICT (idempotency_key, user_id) DO NOTHING
                """, idempotencyKey, userId, STATUS_IN_PROGRESS);
        return inserted == 1;
    }

    /** Текущее состояние приёма: [status, event_id, accepted_count], либо пусто. */
    public List<Row> find(String idempotencyKey, UUID userId) {
        return jdbcTemplate.query("""
                SELECT status, event_id, accepted_count FROM metric.ingest_idempotency
                WHERE idempotency_key = ? AND user_id = ?
                """,
                (resultSet, rowNumber) -> new Row(
                        resultSet.getString("status"),
                        resultSet.getString("event_id"),
                        (Integer) resultSet.getObject("accepted_count")),
                idempotencyKey, userId);
    }

    public void markCompleted(String idempotencyKey, UUID userId, String eventId, int acceptedCount) {
        jdbcTemplate.update("""
                UPDATE metric.ingest_idempotency
                SET status = ?, event_id = ?, accepted_count = ?
                WHERE idempotency_key = ? AND user_id = ?
                """, STATUS_COMPLETED, eventId, acceptedCount, idempotencyKey, userId);
    }

    /** Удаляет запись о незавершённом приёме, чтобы клиент мог повторить после 503. */
    public void delete(String idempotencyKey, UUID userId) {
        jdbcTemplate.update(
                "DELETE FROM metric.ingest_idempotency WHERE idempotency_key = ? AND user_id = ?",
                idempotencyKey, userId);
    }

    /** Строка журнала. */
    public record Row(String status, String eventId, Integer acceptedCount) {
    }
}
