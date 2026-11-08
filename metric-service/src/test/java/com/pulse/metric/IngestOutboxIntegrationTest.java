package com.pulse.metric;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulse.metric.service.IngestOutboxService;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Критерии приёмки P0-2 (устойчивый ingest): расписание отправителя выключено,
 * порции прогоняются вручную; Kafka живая, «падение процесса» моделируется
 * пересозданием контекста поверх того же PostgreSQL.
 *
 * <p>К1 — ack задержан за пределы HTTP-ожидания: ответы не зависят от Kafka и
 * повтор не создаёт второе событие. К2 — падение после приёмa до публикации:
 * новая копия процесса публикует. К3 — падение после ack до закрытия журнала:
 * сверка закрывает статус, повтор не создаёт новую операцию. К4 — параллельные
 * одинаковые ключи дают ровно одну операцию.</p>
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.listener.auto-startup=false",
        "pulse.outbox-scheduling-enabled=false",
        "management.health.defaults.enabled=false"
})
@AutoConfigureMockMvc
@EmbeddedKafka(topics = {"metrics"}, count = 1)
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IngestOutboxIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Value("${spring.embedded.kafka.brokers}")
    private String brokers;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private IngestOutboxService outboxService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private KafkaConsumer<String, String> consumer;

    @BeforeEach
    void subscribe() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "outbox-assert-" + UUID.randomUUID());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of("metrics"));
    }

    @AfterEach
    void closeConsumer() {
        consumer.close();
    }

    @Test
    @Order(1)
    void задержкаAckНеВлияетНа202ИПовторНеСоздаётВтороеСобытие() throws Exception {
        String userId = register("ack-slow@pulse.dev");
        String key = UUID.randomUUID().toString();
        String eventId = ingest(userId, key, "cpu", 1.0);

        // Отправка не выполнялась: событие живёт только в PostgreSQL-очереди.
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM metric.ingest_outbox WHERE event_id = ? AND published_at IS NULL",
                Integer.class, eventId)).isEqualTo(1);

        // Повтор того же ключа и тела — тот же eventId; второе событие не появилось (К1).
        assertThat(ingest(userId, key, "cpu", 1.0)).isEqualTo(eventId);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM metric.ingest_idempotency WHERE user_id = ?",
                Integer.class, UUID.fromString(userId))).isEqualTo(1);

        assertThat(outboxService.publishPending(50, Duration.ofSeconds(5),
                Duration.ofSeconds(1), 5)).isEqualTo(1);
        assertThat(pollEventIds(10)).containsExactly(eventId);

        // Публикация закрыла и очередь, и журнал.
        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM metric.ingest_idempotency WHERE event_id = ?",
                String.class, eventId)).isEqualTo("COMPLETED");
        // Повторная порция пуста (нет переотправки) и повтор ответа идемпотентен.
        assertThat(outboxService.publishPending(50, Duration.ofSeconds(5),
                Duration.ofSeconds(1), 5)).isZero();
        assertThat(ingest(userId, key, "cpu", 1.0)).isEqualTo(eventId);
    }

    @Test
    @Order(2)
    @DirtiesContext(methodMode = DirtiesContext.MethodMode.AFTER_METHOD)
    void падениеПослеПриёмаДоПубликации_публикуетсяНовымПроцессом() throws Exception {
        String userId = register("crash-before-publish@pulse.dev");
        String eventId = ingest(userId, UUID.randomUUID().toString(), "disk", 3.0);
        // «Процесс упал»: публикацию не выполняли, запись осталась в PostgreSQL.
        assertThat(jdbcTemplate.queryForObject(
                "SELECT payload FROM metric.ingest_outbox WHERE event_id = ? AND published_at IS NULL",
                String.class, eventId)).isNotNull();
    }

    @Test
    @Order(3)
    void новаяКопияПроцессаПубликуетОчередьПрошлойЖизни() throws Exception {
        Integer pending = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM metric.ingest_outbox WHERE published_at IS NULL", Integer.class);
        assertThat(pending).isGreaterThan(0);

        assertThat(outboxService.publishPending(50, Duration.ofSeconds(5),
                Duration.ofSeconds(1), 5)).isGreaterThanOrEqualTo(1);
        assertThat(pollEventIds(15)).isNotEmpty();
    }

    @Test
    @Order(4)
    void падениеПослеAckДоЗакрытияЖурналаВосстанавливаетсяСверкой() throws Exception {
        String userId = register("crash-after-ack@pulse.dev");
        String key = UUID.randomUUID().toString();
        String eventId = ingest(userId, key, "gpu", 4.0);

        // Моделируем окно «ack получен, процесс умер до закрытия журнала»:
        // очередь закрыта вручную, журнал остался IN_PROGRESS.
        jdbcTemplate.update("UPDATE metric.ingest_outbox SET published_at = now() WHERE event_id = ?",
                eventId);

        // Повтор клиента: та же операция, новый не создаётся, статус не «навечно IN_PROGRESS».
        assertThat(ingest(userId, key, "gpu", 4.0)).isEqualTo(eventId);

        int reconciled = outboxService.publishPending(50, Duration.ofSeconds(5),
                Duration.ofSeconds(1), 5);
        assertThat(reconciled).isZero(); // уже опубликовано — второй отправки нет
        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM metric.ingest_idempotency WHERE event_id = ?",
                String.class, eventId)).isEqualTo("COMPLETED");
    }

    @Test
    @Order(5)
    void одновременныеЗапросыОднимКлючомСоздаютРовноОднуОперацию() throws Exception {
        String userId = register("concurrent@pulse.dev");
        String key = UUID.randomUUID().toString();
        String body = metricBody("net", 7.0);

        int threads = 4;
        var start = new CountDownLatch(1);
        Callable<String> call = () -> {
            start.await();
            return mockMvc.perform(post("/users/" + userId + "/metrics")
                            .header("Idempotency-Key", key)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body))
                    .andExpect(status().isAccepted())
                    .andReturn().getResponse().getContentAsString();
        };
        var pool = Executors.newFixedThreadPool(threads);
        Set<String> eventIds;
        try {
            var futures = List.of(pool.submit(call), pool.submit(call),
                    pool.submit(call), pool.submit(call));
            start.countDown();
            eventIds = new java.util.HashSet<>();
            for (var future : futures) {
                eventIds.add(objectMapper.readTree(future.get(30, TimeUnit.SECONDS))
                        .get("eventId").asText());
            }
        } finally {
            pool.shutdownNow();
        }
        assertThat(eventIds).as("все параллельные ответы отдали один eventId").hasSize(1);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM metric.ingest_outbox WHERE message_key = ?",
                Integer.class, userId)).isEqualTo(1);
    }

    private String register(String email) throws Exception {
        String body = mockMvc.perform(post("/auth/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(
                                Map.of("email", email, "password", "secret-password"))))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return objectMapper.readTree(body).get("userId").asText();
    }

    private String ingest(String userId, String idempotencyKey, String metricName, double value)
            throws Exception {
        String body = mockMvc.perform(post("/users/" + userId + "/metrics")
                        .header("Idempotency-Key", idempotencyKey)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(metricBody(metricName, value)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.acceptedCount").value(1))
                .andReturn().getResponse().getContentAsString();
        return objectMapper.readTree(body).get("eventId").asText();
    }

    private String metricBody(String name, double value) {
        return "{\"metrics\":[{\"name\":\"" + name + "\",\"value\":" + value
                + ",\"timestamp\":\"2026-11-08T10:00:00Z\"}]}";
    }

    private Set<String> pollEventIds(int timeoutSeconds) throws Exception {
        Set<String> ids = new java.util.HashSet<>();
        long deadline = System.nanoTime() + Duration.ofSeconds(timeoutSeconds).toNanos();
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(400))) {
                ids.add(objectMapper.readTree(record.value()).get("eventId").asText());
            }
        }
        return ids;
    }
}
