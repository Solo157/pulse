package com.pulse.notification;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulse.contracts.event.IncidentDetected;
import com.pulse.contracts.event.SchemaVersions;
import java.time.Duration;
import java.time.Instant;
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
 * Ветка отказа канала доставки (retries/status FAILED): канал недоступен,
 * попытки ограничены, статус фиксируется — событие не зависает и не повторяется вечно.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "pulse.delivery-url=http://127.0.0.1:1/unreachable",
        "pulse.delivery-timeout=300ms",
        "pulse.delivery-max-attempts=2",
        "pulse.delivery-retry-delay=100ms",
        "management.health.defaults.enabled=false"
})
@EmbeddedKafka(topics = "incidents", count = 1)
@Testcontainers
class NotificationFailureIntegrationTest {

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

    @Test
    void недоступныйКаналДаётСтатусFAILEDИОграниченноеЧислоПопыток() throws Exception {
        UUID userId = UUID.randomUUID();
        jdbcTemplate.update("INSERT INTO metric.users (id, email) VALUES (?, ?)", userId,
                "fail-" + userId + "@pulse.dev");
        String eventId = UUID.randomUUID().toString();

        IncidentDetected event = new IncidentDetected(eventId, userId.toString(),
                "temperature-high", userId + ":temperature-high", false, "temperature", 80.0, 99.0,
                Instant.now(), Instant.parse("2026-11-06T10:00:00Z"), SchemaVersions.CURRENT, "test");
        kafkaTemplate.send("incidents", userId.toString(), objectMapper.writeValueAsString(event))
                .get(10, TimeUnit.SECONDS);

        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            Integer attempts = jdbcTemplate.query(
                            "SELECT attempts FROM metric.notifications"
                                    + " WHERE source_event_id = ? AND status = 'FAILED'",
                            (rs, n) -> rs.getInt(1), eventId)
                    .stream().findFirst().orElse(null);
            if (attempts != null) {
                assertThat(attempts).isEqualTo(2);
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Уведомление не переведено в FAILED за отведённое время");
    }
}
