package com.pulse.notification;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulse.contracts.event.IncidentDetected;
import com.pulse.contracts.event.SchemaVersions;
import com.pulse.notification.config.NotificationDeliveryAgeMetrics;
import com.pulse.notification.repository.NotificationRepository;
import io.micrometer.core.instrument.MeterRegistry;
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
 * Гибель процесса посреди доставки (P1-6): строка остаётся {@code DELIVERING},
 * повторное событие Kafka не инициирует новую доставку (claim уже занят) и не
 * меняет строку; возраст такой строки доступен для метрики наблюдения.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "pulse.delivery-url=http://127.0.0.1:1/unreachable",
        "pulse.delivery-timeout=300ms",
        "pulse.delivery-max-attempts=2",
        "pulse.delivery-retry-delay=100ms",
        "pulse.pending-age-refresh=1h",
        "management.health.defaults.enabled=false"
})
@EmbeddedKafka(topics = "incidents", count = 1)
@Testcontainers
class NotificationStuckDeliveryIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
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

    @Autowired
    private NotificationRepository repository;

    @Autowired
    private NotificationDeliveryAgeMetrics ageMetrics;

    @Autowired
    private MeterRegistry meterRegistry;

    @Test
    void повторПослеГибелиПосредиДоставкиНеДоставляетЗановоИНеМеняетСтроку() throws Exception {
        UUID userId = UUID.randomUUID();
        jdbcTemplate.update("INSERT INTO metric.users (id, email) VALUES (?, ?)", userId,
                "stuck-" + userId + "@pulse.dev");
        String eventId = UUID.randomUUID().toString();
        // Симулируем состояние после рестарта: claim выполнен, finish не наступил.
        assertThat(repository.claim(eventId, userId, userId + ":temperature-high",
                "stuck-" + userId + "@pulse.dev")).isTrue();

        kafkaTemplate.send("incidents", userId.toString(),
                        objectMapper.writeValueAsString(incident(eventId, userId)))
                .get(10, TimeUnit.SECONDS);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            if (meterRegistry.find("pulse.notifications").tag("status", "duplicate").counter() != null
                    && meterRegistry.find("pulse.notifications").tag("status", "duplicate")
                            .counter().count() >= 1.0) {
                break;
            }
            Thread.sleep(100);
        }

        Map<String, Object> row = jdbcTemplate.queryForMap(
                "SELECT status, attempts FROM metric.notifications WHERE source_event_id = ?",
                eventId);
        assertThat(row.get("status")).isEqualTo("DELIVERING");
        assertThat(((Number) row.get("attempts")).intValue()).isZero();
        assertThat(meterRegistry.find("pulse.notifications").tag("status", "duplicate")
                .counter().count()).isEqualTo(1.0);

        assertThat(repository.oldestDeliveringAgeSeconds()).isGreaterThan(0.0);
        ageMetrics.refresh();
        assertThat(meterRegistry.find("pulse.notification.oldest.pending.age")
                .gauge().value()).isGreaterThan(0.0);
    }

    private IncidentDetected incident(String eventId, UUID userId) {
        return new IncidentDetected(eventId, userId.toString(), "temperature-high",
                userId + ":temperature-high", false, "temperature", 80.0, 85.0,
                Instant.now(), Instant.parse("2026-11-09T10:00:00Z"),
                SchemaVersions.CURRENT, "test");
    }
}
