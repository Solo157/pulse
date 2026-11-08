package com.pulse.processing.state;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Transactional outbox событий {@code IncidentDetected}: запись в таблицу
 * {@code metric.incident_outbox} выполняется в одной транзакции с переходом
 * state-машины, поэтому событие не теряется при недоступной Kafka.
 *
 * <p>Оповещатель выбирает порцию чернованием {@code FOR UPDATE SKIP LOCKED}
 * с ограниченным по времени держанием ({@code lock_until}): два экземпляра сервиса
 * не публикуют одну запись одновременно, а просроченное держание (упавший
 * оповещатель) освобождает запись следующему проходу.</p>
 */
@Repository
public class OutboxRepository {

    private static final int MAX_ERROR_LENGTH = 500;

    private final JdbcTemplate jdbcTemplate;

    public OutboxRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /** Непубликованная запись outbox, отданная оповещателю; attempts — уже с учётом этой попытки. */
    public record Pending(UUID id, String messageKey, String payload, int attempts) {
    }

    /**
     * Ставит сериализованное событие в очередь. Идемпотентна по {@code id}:
     * повторная доставка пакета метрик, уже зафиксировавшая переход, сюда не попадает,
     * но конфликт primary key на всякий случай гасится — событие с таким id уже в очереди.
     */
    public void enqueue(UUID id, String messageKey, String payload) {
        jdbcTemplate.update("""
                INSERT INTO metric.incident_outbox (id, message_key, payload)
                VALUES (?, ?, ?)
                ON CONFLICT (id) DO NOTHING
                """, id, messageKey, payload);
    }

    /** Забирает не более {@code limit} готовых к попытке записей и держит их {@code lock}. */
    public List<Pending> claim(int limit, Duration lock) {
        return jdbcTemplate.query("""
                UPDATE metric.incident_outbox
                SET attempts = attempts + 1, lock_until = now() + (? * interval '1 millisecond')
                WHERE id IN (
                    SELECT id FROM metric.incident_outbox
                    WHERE published_at IS NULL
                      AND next_attempt_at <= now()
                      AND (lock_until IS NULL OR lock_until <= now())
                    ORDER BY created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT ?)
                RETURNING id, message_key, payload, attempts
                """, (rs, rowNum) -> new Pending(rs.getObject("id", UUID.class),
                        rs.getString("message_key"), rs.getString("payload"),
                        rs.getInt("attempts")),
                lock.toMillis(), limit);
    }

    /** Подтверждение брокера получено — запись закрывается навсегда. */
    public void markPublished(UUID id) {
        jdbcTemplate.update("UPDATE metric.incident_outbox SET published_at = now() WHERE id = ?", id);
    }

    /** Неудача: запись возвращается в очередь после backoff, держание снимается. */
    public void markFailed(UUID id, Duration backoff, String error) {
        String truncated = error == null ? null
                : error.substring(0, Math.min(MAX_ERROR_LENGTH, error.length()));
        jdbcTemplate.update("""
                UPDATE metric.incident_outbox
                SET next_attempt_at = now() + (? * interval '1 millisecond'),
                    last_error = ?, lock_until = NULL
                WHERE id = ?
                """, backoff.toMillis(), truncated, id);
    }

    /** Очередь на публикации — для метрики глубины. */
    public long pendingCount() {
        Long count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM metric.incident_outbox WHERE published_at IS NULL", Long.class);
        return count == null ? 0 : count;
    }

    /** Возраст самой старой непроявленной записи (0, если очередь пуста) — метрика задержки. */
    public Duration oldestPendingAge() {
        Double maxLagSeconds = jdbcTemplate.queryForObject("""
                SELECT extract(epoch FROM max(now() - created_at))
                FROM metric.incident_outbox WHERE published_at IS NULL
                """, Double.class);
        return maxLagSeconds == null ? Duration.ZERO
                : Duration.ofMillis(Math.round(maxLagSeconds * 1000));
    }

    /** Удаляет опубликованное старше {@code retention}; возвращает число удалённых строк. */
    public int deletePublishedOlderThan(Instant cutoff) {
        return jdbcTemplate.update(
                "DELETE FROM metric.incident_outbox WHERE published_at < ?", Timestamp.from(cutoff));
    }
}
