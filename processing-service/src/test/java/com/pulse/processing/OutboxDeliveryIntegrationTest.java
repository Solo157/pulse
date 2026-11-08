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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Доставка событий из transactional outbox при живой Kafka: опубликованный payload
 * байт-в-байт равен записанному (стабильный eventId), повторная порция не дублирует,
 * переживание «перезапуска» сервиса (новая копия контекста над тем же PostgreSQL),
 * и параллельные оповещатели не публикуют одну запись дважды (FOR UPDATE SKIP LOCKED).
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.listener.auto-startup=false",
        "pulse.outbox-scheduling-enabled=false",
        "pulse.rule-cache-ttl=2s",
        "management.health.defaults.enabled=false"
})
@EmbeddedKafka(topics = {"incidents"}, count = 1)
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class OutboxDeliveryIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    private static final Instant T0 = Instant.parse("2026-11-08T10:00:00Z");

    @Value("${spring.embedded.kafka.brokers}")
    private String brokers;

    @Autowired
    private IncidentEvaluator evaluator;

    @Autowired
    private OutboxService outboxService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private Consumer<String, String> incidentReader;

    @BeforeEach
    void startReader() {
        incidentReader = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ConsumerConfig.GROUP_ID_CONFIG, "outbox-reader-" + UUID.randomUUID(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
        List<TopicPartition> incidents = incidentReader.partitionsFor("incidents",
                        Duration.ofSeconds(30)).stream()
                .map(info -> new TopicPartition(info.topic(), info.partition())).toList();
        incidentReader.assign(incidents);
        incidentReader.seekToBeginning(incidents);
    }

    @AfterEach
    void stopReader() {
        incidentReader.close();
    }

    @Test
    @Order(1)
    @DirtiesContext(methodMode = DirtiesContext.MethodMode.AFTER_METHOD)
    void публикуетсяРовноЗаписанныйPayloadИПовторнаяПорцияПустая() throws Exception {
        UUID userId = UUID.randomUUID();
        evaluator.process(metricEvent(userId, 85.0, T0));

        String persisted = jdbcTemplate.queryForObject(
                "SELECT payload FROM metric.incident_outbox WHERE message_key = ?",
                String.class, userId.toString());
        assertThat(persisted).isNotNull();

        assertThat(outboxService.publishPending(50, Duration.ofSeconds(5), Duration.ofSeconds(2), 5))
                .isEqualTo(1);
        String delivered = null;
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (delivered == null && System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : incidentReader.poll(Duration.ofMillis(500))) {
                if (record.key().equals(userId.toString())) {
                    delivered = record.value();
                    break;
                }
            }
        }
        // Опубликованный payload байт-в-байт равен записанному в outbox (стабильный eventId).
        assertThat(delivered).isEqualTo(persisted);

        // Повторная порция: опубликованное не переопубликовывается.
        assertThat(outboxService.publishPending(50, Duration.ofSeconds(5), Duration.ofSeconds(2), 5))
                .isZero();
    }

    @Test
    @Order(2)
    @DirtiesContext(methodMode = DirtiesContext.MethodMode.AFTER_METHOD)
    void очередьПереживаетПерезапускСервисаИДоставляется() throws Exception {
        UUID userId = UUID.randomUUID();
        evaluator.process(metricEvent(userId, 85.0, T0));
        // Из «упавшего» процесса ничего не публиковалось — только запись в БД.
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM metric.incident_outbox WHERE message_key = ?"
                        + " AND published_at IS NULL", Integer.class, userId.toString())).isEqualTo(1);
    }

    @Test
    @Order(3)
    void новаяКопияСервисаДобираетОчередьПрошлойЖизни() throws Exception {
        Integer pendingBefore = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM metric.incident_outbox WHERE published_at IS NULL", Integer.class);
        assertThat(pendingBefore).isGreaterThan(0);

        int published = outboxService.publishPending(50, Duration.ofSeconds(5), Duration.ofSeconds(2), 5);
        assertThat(published).isGreaterThanOrEqualTo(1);

        List<String> eventIds = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (ConsumerRecord<String, String> record : drainConsumer(10)) {
            IncidentDetected event = objectMapper.readValue(record.value(), IncidentDetected.class);
            assertThat(seen.add(event.eventId())).as("EVENT %s дважды", event.eventId()).isTrue();
            eventIds.add(event.eventId());
        }
        assertThat(eventIds).hasSizeGreaterThanOrEqualTo(published);
    }

    @Test
    @Order(4)
    void параллельныеОповещателиНеДублируютЗаписи() throws Exception {
        UUID userId = UUID.randomUUID();
        for (int i = 0; i < 8; i++) {
            outboxService.enqueue(new IncidentDetected(UUID.randomUUID().toString(),
                    userId.toString(), "temperature-high", userId + ":temperature-high",
                    i % 2 == 1, "temperature", 80.0, 85.0, Instant.now(), T0.plusSeconds(i * 60L),
                    SchemaVersions.CURRENT, "parallel-" + i));
        }

        var pool = Executors.newFixedThreadPool(4);
        Callable<Integer> drain = () ->
                outboxService.publishPending(50, Duration.ofSeconds(5), Duration.ofSeconds(2), 5);
        int total = 0;
        try {
            for (var future : pool.invokeAll(List.of(drain, drain, drain, drain))) {
                total += future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        assertThat(total).as("8 записей — 8 публикаций суммарно").isEqualTo(8);

        Set<String> ownEventIds = new HashSet<>();
        for (ConsumerRecord<String, String> record : drainConsumer(15)) {
            IncidentDetected event = objectMapper.readValue(record.value(), IncidentDetected.class);
            if (event.userId().equals(userId.toString())) {
                assertThat(ownEventIds.add(event.eventId())).as("дубль события в топике").isTrue();
            }
        }
        assertThat(ownEventIds).hasSize(8);
    }

    private List<ConsumerRecord<String, String>> drainConsumer(int timeoutSeconds) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        long deadline = System.nanoTime() + Duration.ofSeconds(timeoutSeconds).toNanos();
        while (System.nanoTime() < deadline) {
            incidentReader.poll(Duration.ofMillis(500)).forEach(records::add);
        }
        return records;
    }

    private MetricEvent metricEvent(UUID userId, double value, Instant at) {
        return new MetricEvent(UUID.randomUUID().toString(), userId.toString(),
                SchemaVersions.CURRENT, Instant.now(), "test-" + UUID.randomUUID(),
                List.of(new MetricPoint("temperature", value, at)));
    }
}
