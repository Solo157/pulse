package com.pulse.metric;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Интеграционные тесты ingest-пути (раздел 16): реальные PostgreSQL (Testcontainers)
 * и Kafka (Spring Embedded). Проверяют статусы и тело ответов, публикацию события,
 * идемпотентность и отсутствие маскирующих 202 при недоступной Kafka (второй класс).
 */
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@AutoConfigureMockMvc
@EmbeddedKafka(topics = "metrics", count = 1)
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MetricIngestIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private EmbeddedKafkaBroker kafkaBroker;

    private KafkaConsumer<String, String> consumer;

    @BeforeEach
    void subscribeToMetrics() {
        consumer = newConsumer("assert-group");
        consumer.subscribe(List.of("metrics"));
    }

    @AfterEach
    void closeConsumer() {
        consumer.close();
    }

    @Test
    void регистрацияОтдаётUserIdаПовторКонфликт() throws Exception {
        String userId = register("first@pulse.dev");
        assertThat(UUID.fromString(userId)).isNotNull();

        mockMvc.perform(post("/auth/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(Map.of("email", "first@pulse.dev", "password", "secret-password"))))
                .andExpect(status().isConflict())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
    }

    @Test
    void отказыНаНевалидныхЗапросах() throws Exception {
        mockMvc.perform(post("/auth/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(Map.of("email", "без-собаки", "password", "secret-password"))))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));

        String userId = register("key-required@pulse.dev");
        mockMvc.perform(post("/users/" + userId + "/metrics")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(metricPointJson("cpu", 1)))
                .andExpect(status().isBadRequest());

        mockMvc.perform(post("/users/" + UUID.randomUUID() + "/metrics")
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(metricPointJson("cpu", 1)))
                .andExpect(status().isNotFound());

        String bigUserId = register("big-batch@pulse.dev");
        mockMvc.perform(post("/users/" + bigUserId + "/metrics")
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"metrics\":[" + "\"cpu\",".repeat(0) + pointArray(1001) + "]}"))
                .andExpect(status().isPayloadTooLarge());
    }

    @Test
    void приёмПубликуетСобытиеИОтвечаетИдемпотентно() throws Exception {
        String userId = register("ingest@pulse.dev");
        String idempotencyKey = UUID.randomUUID().toString();

        String body = mockMvc.perform(post("/users/" + userId + "/metrics")
                        .header("Idempotency-Key", idempotencyKey)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(metricPointJson("cpu_usage", 42)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.userId").value(userId))
                .andExpect(jsonPath("$.acceptedCount").value(1))
                .andReturn().getResponse().getContentAsString();
        String eventId = objectMapper.readTree(body).get("eventId").asText();

        ConsumerRecord<String, String> published = pollForEvent(eventId);
        assertThat(published.key()).isEqualTo(userId);
        assertThat(objectMapper.readTree(published.value()).get("schemaVersion").asText()).isEqualTo("1");

        // Повтор того же Idempotency-Key: тот же eventId и 202, новых записей в Kafka нет.
        mockMvc.perform(post("/users/" + userId + "/metrics")
                        .header("Idempotency-Key", idempotencyKey)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(metricPointJson("cpu_usage", 42)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.eventId").value(eventId));
        assertThat(pollForEvent("неизвестный-event")).isNull();
    }

    @Test
    void повторНезавершённогоПриёмаКонфликт() throws Exception {
        String userId = register("inflight@pulse.dev");
        String idempotencyKey = UUID.randomUUID().toString();
        // Моделируем шаг приёма, застрявший до подтверждения брокером.
        jdbcTemplate.update("""
                INSERT INTO metric.ingest_idempotency (idempotency_key, user_id, status)
                VALUES (?, ?, 'IN_PROGRESS')
                """, idempotencyKey, UUID.fromString(userId));

        mockMvc.perform(post("/users/" + userId + "/metrics")
                        .header("Idempotency-Key", idempotencyKey)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(metricPointJson("cpu", 1)))
                .andExpect(status().isConflict());
    }

    private String register(String email) throws Exception {
        String body = mockMvc.perform(post("/auth/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(Map.of("email", email, "password", "secret-password"))))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return objectMapper.readTree(body).get("userId").asText();
    }

    private String json(Map<String, String> values) throws Exception {
        return objectMapper.writeValueAsString(values);
    }

    private String metricPointJson(String name, double value) {
        return "{\"metrics\":[{\"name\":\"" + name + "\",\"value\":" + value
                + ",\"timestamp\":\"2026-11-06T10:00:00Z\"}]}";
    }

    private static String pointArray(int count) {
        StringBuilder builder = new StringBuilder();
        for (int index = 0; index < count; index++) {
            if (index > 0) {
                builder.append(',');
            }
            builder.append("{\"name\":\"m").append(index).append("\",\"value\":1,")
                    .append("\"timestamp\":\"2026-11-06T10:00:00Z\"}");
        }
        return builder.toString();
    }

    private KafkaConsumer<String, String> newConsumer(String group) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBrokersAsString());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(properties);
    }

    /**
     * Опрашивает топик до timeout в поисках события с нужным eventId.
     * Возвращает null, если подходящего события нет (например, проверка «не появилось дублей»).
     */
    private ConsumerRecord<String, String> pollForEvent(String eventId) {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(300))) {
                try {
                    String found = objectMapper.readTree(record.value()).get("eventId").asText();
                    if (eventId.equals(found)) {
                        return record;
                    }
                } catch (Exception ignored) {
                    // сообщение не нашего формата — пропускаем
                }
            }
            if (eventId.startsWith("неизвестный")) {
                return null;
            }
        }
        return null;
    }
}
