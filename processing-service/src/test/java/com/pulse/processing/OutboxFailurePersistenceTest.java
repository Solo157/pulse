package com.pulse.processing;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulse.contracts.common.MetricPoint;
import com.pulse.contracts.event.IncidentDetected;
import com.pulse.contracts.event.MetricEvent;
import com.pulse.contracts.event.SchemaVersions;
import com.pulse.processing.service.IncidentEvaluator;
import com.pulse.processing.service.OutboxService;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Дефект P0-1: событие инцидента не должно теряться при недоступной Kafka.
 * Kafka намеренно unreachable (bootstrap на закрытый порт), расписание оповещателя
 * выключено — переход state-машины обязан зафиксироваться вместе с записью outbox,
 * а неудачные попытки публикации оставить запись в очереди с паузой повтора.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=127.0.0.1:1",
        "spring.kafka.listener.auto-startup=false",
        "pulse.outbox-scheduling-enabled=false",
        "pulse.publish-ack-timeout=500ms",
        "management.health.defaults.enabled=false"
})
@Testcontainers
class OutboxFailurePersistenceTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    private static final Instant T0 = Instant.parse("2026-11-06T10:00:00Z");
    private static final Duration ACK = Duration.ofMillis(500);

    @Autowired
    private IncidentEvaluator evaluator;

    @Autowired
    private OutboxService outboxService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private UUID userId;

    @BeforeEach
    void freshUser() {
        userId = UUID.randomUUID();
    }

    @Test
    void переходСовершаетсяИСобытиеОстаетсяВОчередиПриНедоступнойKafka() throws Exception {
        evaluator.process(metricEvent(85.0, T0));

        assertThat(jdbcTemplate.queryForObject(
                "SELECT open FROM metric.incident_state WHERE incident_key = ?", Boolean.class,
                userId + ":temperature-high")).isTrue();

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(
                "SELECT payload, published_at, attempts, next_attempt_at, last_error, lock_until"
                        + " FROM metric.incident_outbox WHERE message_key = ?", userId.toString());
        assertThat(rows).hasSize(1);
        Map<String, Object> row = rows.get(0);
        assertThat(row.get("published_at")).isNull();
        IncidentDetected persisted = objectMapper.readValue(
                (String) row.get("payload"), IncidentDetected.class);
        assertThat(persisted.close()).isFalse();
        assertThat(persisted.ruleId()).isEqualTo("temperature-high");

        int published = drainOnce();
        assertThat(published).isZero();

        row = jdbcTemplate.queryForMap(
                "SELECT attempts, now() < next_attempt_at AS delayed, last_error, lock_until"
                        + " FROM metric.incident_outbox WHERE message_key = ?", userId.toString());
        assertThat(row.get("attempts")).isEqualTo(1);
        assertThat(row.get("delayed")).isEqualTo(true);
        assertThat(row.get("last_error")).isNotNull();
        assertThat(row.get("lock_until")).isNull();

        // Неудача не теряет событие и не плодит второй event при повторной обработке пакета.
        evaluator.process(metricEvent(85.0, T0));
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM metric.incident_outbox WHERE message_key = ?", Integer.class,
                userId.toString())).isEqualTo(1);

        // Пауза повтора ещё не истекла — порция не забирает запись.
        assertThat(drainOnce()).isZero();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT attempts FROM metric.incident_outbox WHERE message_key = ?", Integer.class,
                userId.toString())).isEqualTo(1);

        // После паузы запись снова доступна и получает вторую попытку.
        Thread.sleep(Duration.ofSeconds(1).toMillis() + 100);
        drainOnce();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT attempts FROM metric.incident_outbox WHERE message_key = ?", Integer.class,
                userId.toString())).isEqualTo(2);
    }

    @Test
    void удержаниеОповещателяИстекаетИЗаписьСноваДоступна() throws Exception {
        evaluator.process(metricEvent(85.0, T0));
        // Чернование с нулевым сроком держания имитирует упавший процесс оповещателя.
        assertThat(outboxService.publishPending(10, Duration.ZERO, ACK, 5)).isZero();

        Integer claimable = jdbcTemplate.queryForObject("""
                SELECT count(*) FROM metric.incident_outbox
                WHERE message_key = ? AND published_at IS NULL
                  AND (lock_until IS NULL OR lock_until <= now())
                """, Integer.class, userId.toString());
        assertThat(claimable).isEqualTo(1);
    }

    private int drainOnce() {
        return outboxService.publishPending(10, Duration.ofSeconds(2), ACK, 5);
    }

    private MetricEvent metricEvent(double value, Instant at) {
        return new MetricEvent(UUID.randomUUID().toString(), userId.toString(),
                SchemaVersions.CURRENT, Instant.now(), "test-" + UUID.randomUUID(),
                List.of(new MetricPoint("temperature", value, at)));
    }
}
