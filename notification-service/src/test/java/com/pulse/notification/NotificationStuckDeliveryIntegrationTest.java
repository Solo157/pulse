package com.pulse.notification;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulse.contracts.event.IncidentDetected;
import com.pulse.contracts.event.SchemaVersions;
import com.pulse.notification.repository.NotificationRepository;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
 * меняет строку.
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
    private NotificationRepository repository;

    @Value("${spring.embedded.kafka.brokers}")
    private String bootstrapServers;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JdbcTemplate jdbcTemplate;

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
        awaitConsumed("notification-group");

        Map<String, Object> row = jdbcTemplate.queryForMap(
                "SELECT status, attempts FROM metric.notifications WHERE source_event_id = ?",
                eventId);
        assertThat(row.get("status")).isEqualTo("DELIVERING");
        assertThat(((Number) row.get("attempts")).intValue()).isZero();
    }

    /** Ждём, пока группа прочитает отправленное событие: инцидент реально обработан повторно. */
    private void awaitConsumed(String groupId) throws Exception {
        TopicPartition partition = new TopicPartition("incidents", 0);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers))) {
            while (System.nanoTime() < deadline) {
                OffsetAndMetadata committed = admin.listConsumerGroupOffsets(groupId)
                        .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS).get(partition);
                if (committed != null && committed.offset() >= 1) {
                    return;
                }
                Thread.sleep(100);
            }
        }
        throw new AssertionError("группа " + groupId + " не прочитала событие из incidents");
    }

    private IncidentDetected incident(String eventId, UUID userId) {
        return new IncidentDetected(eventId, userId.toString(), "temperature-high",
                userId + ":temperature-high", false, "temperature", 80.0, 85.0,
                Instant.now(), Instant.parse("2026-11-09T10:00:00Z"),
                SchemaVersions.CURRENT, "test");
    }
}
