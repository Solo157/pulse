package com.pulse.notification.repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Доступ к {@code metric.notifications} и адресу получателя ({@code metric.users}).
 */
@Repository
public class NotificationRepository {

    private static final String CHANNEL_EMAIL = "EMAIL";

    private final JdbcTemplate jdbcTemplate;

    public NotificationRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /** Электронная почта владельца из реестра пользователей MetricService. */
    public Optional<String> findEmail(UUID userId) {
        return jdbcTemplate.query("SELECT email FROM metric.users WHERE id = ?",
                (rs, n) -> rs.getString(1), userId).stream().findFirst();
    }

    /**
     * Регистрирует уведомление под доставкой. false — такое событие уже обработано
     * (повтор Kafka или гонка реплик): повторная доставка не выполняется.
     */
    public boolean claim(String sourceEventId, UUID userId, String incidentKey, String destination) {
        int inserted = jdbcTemplate.update("""
                INSERT INTO metric.notifications
                    (source_event_id, user_id, incident_key, channel, destination, status, attempts)
                VALUES (?, ?, ?, ?, ?, ?, 0)
                ON CONFLICT (source_event_id) DO NOTHING
                """, sourceEventId, userId, incidentKey, CHANNEL_EMAIL, destination,
                NotificationStatus.DELIVERING.name());
        return inserted == 1;
    }

    /** Итог доставки: attempts — количество выполненных попыток, sent_at только для SENT. */
    public void finish(String sourceEventId, NotificationStatus status, int attempts, Instant sentAt) {
        jdbcTemplate.update("""
                UPDATE metric.notifications
                SET status = ?, attempts = ?, sent_at = ?
                WHERE source_event_id = ?
                """, status.name(), attempts, sentAt == null ? null : Timestamp.from(sentAt),
                sourceEventId);
    }

    /**
     * Возраст самой старой доставки в статусе {@code DELIVERING}, секунд; 0 — таких нет.
     * Нет нулевых строк после успешного запуска сервиса: {@code DELIVERING} остаётся
     * только если процесс умер между claim и finish (строка терминальна для повторов).
     */
    public double oldestDeliveringAgeSeconds() {
        Double age = jdbcTemplate.queryForObject("""
                SELECT COALESCE(EXTRACT(EPOCH FROM (now() - MIN(created_at))), 0)
                FROM metric.notifications
                WHERE status = 'DELIVERING'
                """, Double.class);
        return age == null ? 0 : age;
    }
}
