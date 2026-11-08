package com.pulse.processing;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulse.contracts.common.MetricPoint;
import com.pulse.contracts.event.IncidentDetected;
import com.pulse.contracts.event.MetricEvent;
import com.pulse.contracts.event.SchemaVersions;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
 * Покрытие Этапа 3 (раздел 8 / FR-3): реальные Kafka и PostgreSQL.
 * Правило {@code temperature-high} (value &gt; 80) засевается schema.sql ProcessingService.
 * Каждый тест работает своим пользователем; читатель incidents отфильтровывает
 * события других тестов по ключу инцидента.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "pulse.close-cooldown=10s",
        "pulse.rule-cache-ttl=2s",
        "pulse.outbox-poll-interval=200ms",
        "management.health.defaults.enabled=false"
})
@EmbeddedKafka(topics = {"metrics", "incidents"}, count = 1)
@Testcontainers
class IncidentProcessingIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    private static final Instant T0 = Instant.parse("2026-11-06T10:00:00Z");

    @Value("${spring.embedded.kafka.brokers}")
    private String brokers;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private UUID userId;
    private Consumer<String, String> incidentReader;

    @BeforeEach
    void startReader() {
        userId = UUID.randomUUID();
        incidentReader = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ConsumerConfig.GROUP_ID_CONFIG, "reader-" + userId,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
        List<TopicPartition> incidents = incidentReader.partitionsFor("incidents").stream()
                .map(info -> new TopicPartition(info.topic(), info.partition())).toList();
        incidentReader.assign(incidents);
        incidentReader.seekToBeginning(incidents);
    }

    @AfterEach
    void stopReader() {
        incidentReader.close();
    }

    @Test
    void нарушениеОткрываетОдинИнцидентИЗаписываетЕгоVBД() throws Exception {
        sendTemperature(85.0, T0);

        IncidentDetected openEvent = pollOwnIncident(30);
        assertThat(openEvent).isNotNull();
        assertThat(openEvent.close()).isFalse();
        assertThat(openEvent.ruleId()).isEqualTo("temperature-high");
        assertThat(openEvent.incidentKey()).isEqualTo(userId + ":temperature-high");
        assertThat(openEvent.value()).isEqualTo(85.0);
        assertThat(openEvent.threshold()).isEqualTo(80.0);

        awaitRowStatus("OPEN");
        // Следующая нарушающая точка того же периода — без второго события и без новой строки.
        sendTemperature(90.0, T0.plusSeconds(30));
        assertThat(pollOwnIncident(3)).isNull();
        assertThat(rowCount()).isEqualTo(1);
    }

    @Test
    void закрытиеПослеCooldownаФиксируетсяVBД() throws Exception {
        sendTemperature(85.0, T0);
        pollOwnIncident(30);

        sendTemperature(90.0, T0.plusSeconds(20));
        sendTemperature(50.0, T0.plusSeconds(35)); // 15 c после нарушения >= cooldown 10 c

        IncidentDetected closeEvent = pollOwnIncident(30);
        assertThat(closeEvent).isNotNull();
        assertThat(closeEvent.close()).isTrue();
        assertThat(closeEvent.value()).isNull();

        awaitRowStatus("CLOSED");
        assertThat(rowStatus()).isEqualTo("CLOSED");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT closed_at FROM metric.incidents WHERE user_id = ?", Timestamp.class, userId))
                .isNotNull();
    }

    @Test
    void нормализацияРаньшеCooldownаИнцидентНеЗакрывает() throws Exception {
        sendTemperature(85.0, T0);
        pollOwnIncident(30);

        sendTemperature(20.0, T0.plusSeconds(5)); // раньше cooldown 10 с

        assertThat(pollOwnIncident(2)).isNull();
        assertThat(rowStatus()).isEqualTo("OPEN");
    }

    @Test
    void устаревшаяТочкаИгнорируетсяБезИзмененияИнцидента() throws Exception {
        sendTemperature(85.0, T0);
        pollOwnIncident(30);
        awaitRowStatus("OPEN");
        Timestamp openedAt = rowOpenedAt();

        // Точка раньше уже осмотренной — не продлевает нарушение и не порождает событий.
        sendTemperature(95.0, T0.minusSeconds(120));
        assertThat(pollOwnIncident(2)).isNull();
        assertThat(rowOpenedAt()).isEqualTo(openedAt);
    }

    @Test
    void повторнаяДоставкаIncidentDetectedНеПортитСтрокуИнцидента() throws Exception {
        IncidentDetected original = new IncidentDetected(
                UUID.randomUUID().toString(), userId.toString(), "temperature-high",
                userId + ":temperature-high", false, "temperature", 80.0, 85.0,
                Instant.now(), T0, SchemaVersions.CURRENT, "replay-test");
        String payload = objectMapper.writeValueAsString(original);
        kafkaTemplate.send("incidents", userId.toString(), payload).get(10, TimeUnit.SECONDS);

        awaitRowStatus("OPEN");
        Timestamp openedAt = rowOpenedAt();
        // Вторая доставка того же события (моделирует redelivery Kafka) идемпотентна.
        kafkaTemplate.send("incidents", userId.toString(), payload).get(10, TimeUnit.SECONDS);
        Thread.sleep(1500);

        assertThat(rowCount()).isEqualTo(1);
        assertThat(rowOpenedAt()).isEqualTo(openedAt);
    }

    private void sendTemperature(double value, Instant at) throws Exception {
        MetricEvent event = new MetricEvent(UUID.randomUUID().toString(), userId.toString(),
                SchemaVersions.CURRENT, Instant.now(), "test-" + UUID.randomUUID(),
                List.of(new MetricPoint("temperature", value, at)));
        kafkaTemplate.send("metrics", userId.toString(), objectMapper.writeValueAsString(event))
                .get(10, TimeUnit.SECONDS);
    }

    /** Следующее событие incidents этого пользователя либо null по таймауту. */
    private IncidentDetected pollOwnIncident(int timeoutSeconds) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(timeoutSeconds).toNanos();
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : incidentReader.poll(Duration.ofMillis(500))) {
                IncidentDetected event = objectMapper.readValue(record.value(), IncidentDetected.class);
                if (event.userId().equals(userId.toString())) {
                    return event;
                }
            }
        }
        return null;
    }

    private void awaitRowStatus(String expectedStatus) {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            if (expectedStatus.equals(rowStatusOrNull())) {
                return;
            }
            sleepQuietly(200);
        }
        throw new AssertionError("Инцидент пользователя " + userId + " не переведён в " + expectedStatus);
    }

    private String rowStatus() {
        String status = rowStatusOrNull();
        assertThat(status).as("строка инцидента существует").isNotNull();
        return status;
    }

    private String rowStatusOrNull() {
        return jdbcTemplate.query("SELECT status FROM metric.incidents WHERE user_id = ?",
                (rs, n) -> rs.getString(1), userId).stream().findFirst().orElse(null);
    }

    private Timestamp rowOpenedAt() {
        return jdbcTemplate.queryForObject(
                "SELECT opened_at FROM metric.incidents WHERE user_id = ?", Timestamp.class, userId);
    }

    private int rowCount() {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM metric.incidents WHERE user_id = ?", Integer.class, userId);
        return count == null ? 0 : count;
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
