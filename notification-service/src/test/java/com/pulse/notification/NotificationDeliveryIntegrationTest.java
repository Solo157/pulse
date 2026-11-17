package com.pulse.notification;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulse.contracts.event.IncidentDetected;
import com.pulse.contracts.event.SchemaVersions;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Успешная демонстрационная доставка (FR-5): канал — собственный endpoint сервиса,
 * URL собирается на {@code local.server.port} запущенного веб-окружения.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "pulse.delivery-url=http://localhost:${local.server.port}/internal/demo-receiver",
        "pulse.delivery-retry-delay=100ms",
        "management.health.defaults.enabled=false"
})
@EmbeddedKafka(topics = "incidents", count = 1)
@Testcontainers
class NotificationDeliveryIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            // Пользователи принадлежат MetricService — здесь только минимум для теста.
            .withInitScript("test-users.sql");

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void открытиеИнцидентаСоздаётУведомлениеСоСтатусомSENT() throws Exception {
        UUID userId = createUser("sent-" + UUID.randomUUID() + "@pulse.dev");
        String eventId = UUID.randomUUID().toString();
        send(incident(eventId, userId, false));

        awaitStatus(eventId, "SENT");
        Map<String, Object> row = jdbcTemplate.queryForMap(
                "SELECT status, destination, attempts, sent_at FROM metric.notifications"
                        + " WHERE source_event_id = ?", eventId);
        assertThat(row.get("status")).isEqualTo("SENT");
        assertThat((String) row.get("destination")).endsWith("@pulse.dev");
        assertThat(((Number) row.get("attempts")).intValue()).isGreaterThanOrEqualTo(1);
        assertThat(row.get("sent_at")).isNotNull();

        // Повторная доставка того же события: ни второго уведомления, ни повторной доставки.
        send(incident(eventId, userId, false));
        Thread.sleep(1500);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM metric.notifications WHERE source_event_id = ?", Integer.class,
                eventId)).isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT attempts FROM metric.notifications WHERE source_event_id = ?", Integer.class,
                eventId)).isEqualTo(((Number) row.get("attempts")).intValue());
    }

    @Test
    void закрытиеИнцидентаУведомлениеНеСоздаёт() throws Exception {
        UUID userId = createUser("close-" + UUID.randomUUID() + "@pulse.dev");
        send(incident(UUID.randomUUID().toString(), userId, true));
        Thread.sleep(1500);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM metric.notifications WHERE user_id = ?", Integer.class, userId))
                .isZero();
    }

    private IncidentDetected incident(String eventId, UUID userId, boolean close) {
        return new IncidentDetected(eventId, userId.toString(), "temperature-high",
                userId + ":temperature-high", close, "temperature", 80.0, close ? null : 85.0,
                Instant.now(), close ? null : Instant.parse("2026-11-06T10:00:00Z"),
                SchemaVersions.CURRENT, "test");
    }

    private void send(IncidentDetected event) throws Exception {
        kafkaTemplate.send("incidents", event.userId(), objectMapper.writeValueAsString(event))
                .get(10, TimeUnit.SECONDS);
    }

    private UUID createUser(String email) {
        UUID id = UUID.randomUUID();
        jdbcTemplate.update("INSERT INTO metric.users (id, email) VALUES (?, ?)", id, email);
        return id;
    }

    private void awaitStatus(String sourceEventId, String expected) {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            String status = jdbcTemplate.query(
                    "SELECT status FROM metric.notifications WHERE source_event_id = ?",
                    (rs, n) -> rs.getString(1), sourceEventId).stream().findFirst().orElse(null);
            if (expected.equals(status)) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        throw new AssertionError("Уведомление " + sourceEventId + " не в статусе " + expected);
    }
}
