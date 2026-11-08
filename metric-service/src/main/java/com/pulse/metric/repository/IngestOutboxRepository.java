package com.pulse.metric.repository;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Transactional outbox исходящих {@code MetricEvent} (таблица metric.ingest_outbox):
 * запись создаётся одной транзакцией с журналом идемпотентности ingest, HTTP 202
 * подтверждает устойчивый приём сюда. Отправитель добирает события в Kafka
 * at-least-once: чернование порции {@code FOR UPDATE SKIP LOCKED} с держанием
 * {@code lock_until} (переживает падение отправителя),eventId — primary key,
 * повторная доставка байт-в-байт идентична.
 */
@Repository
public class IngestOutboxRepository {

    private static final int MAX_ERROR_LENGTH = 500;

    private final JdbcTemplate jdbcTemplate;

    public IngestOutboxRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /** Непубликованная запись, отданная отправителю; attempts — уже с учётом этой попытки. */
    public record Pending(String eventId, String messageKey, String payload, int attempts) {
    }

    /** Ставит сериализованное событие в очередь (в транзакции создания приёма). */
    public void enqueue(String eventId, String messageKey, String payload) {
        jdbcTemplate.update("""
                INSERT INTO metric.ingest_outbox (event_id, message_key, payload)
                VALUES (?, ?, ?)
                ON CONFLICT (event_id) DO NOTHING
                """, eventId, messageKey, payload);
    }

    /** Забирает не более {@code limit} готовых к попытке записей и держит их {@code lock}. */
    public List<Pending> claim(int limit, Duration lock) {
        return jdbcTemplate.query("""
                UPDATE metric.ingest_outbox
                SET attempts = attempts + 1, lock_until = now() + (? * interval '1 millisecond')
                WHERE event_id IN (
                    SELECT event_id FROM metric.ingest_outbox
                    WHERE published_at IS NULL
                      AND next_attempt_at <= now()
                      AND (lock_until IS NULL OR lock_until <= now())
                    ORDER BY created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT ?)
                RETURNING event_id, message_key, payload, attempts
                """, (rs, rowNum) -> new Pending(rs.getString("event_id"),
                        rs.getString("message_key"), rs.getString("payload"), rs.getInt("attempts")),
                lock.toMillis(), limit);
    }

    /**
     * Брокер подтвердил: закрывает запись outbox и соответствующий приём
     * (один вызывающий транзакционный шаг, журнал не может остаться IN_PROGRESS).
     */
    public void markPublished(String eventId) {
        jdbcTemplate.update(
                "UPDATE metric.ingest_outbox SET published_at = now() WHERE event_id = ?", eventId);
    }

    /** Неудача: запись возвращается в очередь после паузы, держание снимается. */
    public void markFailed(String eventId, Duration backoff, String error) {
        String truncated = error == null ? null
                : error.substring(0, Math.min(MAX_ERROR_LENGTH, error.length()));
        jdbcTemplate.update("""
                UPDATE metric.ingest_outbox
                SET next_attempt_at = now() + (? * interval '1 millisecond'),
                    last_error = ?, lock_until = NULL
                WHERE event_id = ?
                """, backoff.toMillis(), truncated, eventId);
    }

    /** Очередь на публикации — для метрики глубины. */
    public long pendingCount() {
        Long count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM metric.ingest_outbox WHERE published_at IS NULL", Long.class);
        return count == null ? 0 : count;
    }

    /** Возраст самой старой непроявленной записи (0, если очередь пуста). */
    public Duration oldestPendingAge() {
        Double maxLagSeconds = jdbcTemplate.queryForObject("""
                SELECT extract(epoch FROM max(now() - created_at))
                FROM metric.ingest_outbox WHERE published_at IS NULL
                """, Double.class);
        return maxLagSeconds == null ? Duration.ZERO
                : Duration.ofMillis(Math.round(maxLagSeconds * 1000));
    }

    /** Политика хранения: опубликованные записи старше {@code retention} удаляются. */
    public int deletePublishedOlderThan(Duration retention) {
        return jdbcTemplate.update("DELETE FROM metric.ingest_outbox WHERE published_at < ?",
                Timestamp.from(Instant.now().minus(retention)));
    }
}
