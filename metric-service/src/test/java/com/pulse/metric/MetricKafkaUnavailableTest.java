package com.pulse.metric;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * P0-2, критерий 7: HTTP 202 означает устойчивый приём (запись в PostgreSQL-очередь),
 * а не попытку отправить сообщение. При недостижимом брокере приём остаётся 202,
 * событие ждёт в ingest_outbox, повтор ключа отвечает тем же eventId.
 * Отправка из очереди здесь невозможна и лишь копит попытки с паузами.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=localhost:2",
        "spring.kafka.producer.properties.max.block.ms=500",
        "spring.kafka.listener.auto-startup=false",
        "pulse.publish-ack-timeout=500ms",
        "pulse.outbox-scheduling-enabled=false",
        "management.health.defaults.enabled=false"
})
@AutoConfigureMockMvc
@Testcontainers
class MetricKafkaUnavailableTest {

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

    @Test
    void приёмУстойчивПриНедоступнойKafkaИНеТребуетНовогоКлюча() throws Exception {
        String body = mockMvc.perform(post("/auth/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(
                                Map.of("email", "kafka-down@pulse.dev", "password", "secret-password"))))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String userId = objectMapper.readTree(body).get("userId").asText();
        String key = UUID.randomUUID().toString();
        String metric = "{\"metrics\":[{\"name\":\"cpu\",\"value\":1,"
                + "\"timestamp\":\"2026-11-06T10:00:00Z\"}]}";

        String accepted = mockMvc.perform(post("/users/" + userId + "/metrics")
                        .header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(metric))
                .andExpect(status().isAccepted())
                .andReturn().getResponse().getContentAsString();
        String eventId = objectMapper.readTree(accepted).get("eventId").asText();

        // Событие устойчиво: журнал IN_PROGRESS закрыт не был, payload ждёт в очереди.
        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM metric.ingest_idempotency WHERE idempotency_key = ?",
                String.class, key)).isEqualTo("IN_PROGRESS");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT payload FROM metric.ingest_outbox WHERE event_id = ?", String.class, eventId))
                .contains("\"eventId\":\"" + eventId);

        // Повтор того же ключа и тела — тот же eventId, новых записей нет, 409 нет.
        mockMvc.perform(post("/users/" + userId + "/metrics")
                        .header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(metric))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.eventId").value(eventId));
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM metric.ingest_outbox WHERE message_key = ?",
                Integer.class, userId)).isEqualTo(1);
    }
}
