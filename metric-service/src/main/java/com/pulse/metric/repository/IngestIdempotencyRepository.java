package com.pulse.metric.repository;

import java.time.Duration;
import java.time.Instant;
import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Журнал идемпотентности ingest (таблица metric.ingest_idempotency).
 * Один шаг "приём" на пару (ключ, пользователь): eventId, число точек и
 * отпечаток тела фиксируются при создании, поэтому устойчивый ответ клиенту
 * не зависит от того, подтвердил ли уже брокер событие из outbox.
 */
@Repository
public class IngestIdempotencyRepository {

    public static final String STATUS_IN_PROGRESS = "IN_PROGRESS";
    public static final String STATUS_COMPLETED = "COMPLETED";

    private final JdbcTemplate jdbcTemplate;

    public IngestIdempotencyRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Создаёт запись приёма со статусом IN_PROGRESS. true — если ключа ещё не было;
     * false — ключ занят, дальше работаем с существующей записью.
     */
    public boolean tryCreate(String idempotencyKey, UUID userId, String eventId,
            int acceptedCount, String requestHash) {
        int inserted = jdbcTemplate.update("""
                INSERT INTO metric.ingest_idempotency
                    (idempotency_key, user_id, status, event_id, accepted_count, request_hash)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (idempotency_key, user_id) DO NOTHING
                """, idempotencyKey, userId, STATUS_IN_PROGRESS, eventId, acceptedCount, requestHash);
        return inserted == 1;
    }

    /** Текущее состояние приёма, либо пусто (запись удалена политикой хранения). */
    public List<Row> find(String idempotencyKey, UUID userId) {
        return jdbcTemplate.query("""
                SELECT status, event_id, accepted_count, request_hash FROM metric.ingest_idempotency
                WHERE idempotency_key = ? AND user_id = ?
                """,
                (resultSet, rowNumber) -> new Row(
                        resultSet.getString("status"),
                        resultSet.getString("event_id"),
                        (Integer) resultSet.getObject("accepted_count"),
                        resultSet.getString("request_hash")),
                idempotencyKey, userId);
    }

    /**
     * Подтверждение брокера получено (вызывается одной транзакцией с markPublished outbox).
     * Запись находится по event_id — он зафиксирован в журнале при создании.
     */
    public void markCompleted(String eventId) {
        jdbcTemplate.update(
                "UPDATE metric.ingest_idempotency SET status = ? WHERE event_id = ?",
                STATUS_COMPLETED, eventId);
    }

    /**
     * Восстановление статуса после падения между подтверждением outbox и journal:
     * если событие уже опубликовано, а приём ещё IN_PROGRESS — закрываем приём.
     */
    public int reconcileCompleted() {
        return jdbcTemplate.update("""
                UPDATE metric.ingest_idempotency i SET status = ?
                WHERE i.status = ? AND EXISTS (
                    SELECT 1 FROM metric.ingest_outbox o
                    WHERE o.event_id = i.event_id AND o.published_at IS NOT NULL)
                """, STATUS_COMPLETED, STATUS_IN_PROGRESS);
    }

    /** Политика хранения: удаляет завернённые приёмы старше {@code retention}. */
    public int deleteCompletedOlderThan(Duration retention) {
        return jdbcTemplate.update(
                "DELETE FROM metric.ingest_idempotency WHERE status = ? AND created_at < ?",
                STATUS_COMPLETED, Timestamp.from(Instant.now().minus(retention)));
    }

    /** Строка журнала. */
    public record Row(String status, String eventId, Integer acceptedCount, String requestHash) {
    }
}
