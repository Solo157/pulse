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
 * FR-2 и раздел 9: недоступность Kafka не маскируется 202.
 * Отдельный контекст с недостижимым брокером; ждём короткий таймаут publish-ack.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=localhost:2",
        "spring.kafka.producer.properties.max.block.ms=500",
        "pulse.publish-ack-timeout=500ms",
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
    void регистрацияРаботаетПриНедоступнойKafkaПриёмДаёт503ИНеОставляетЗаблокированныхКлючей()
            throws Exception {
        String body = mockMvc.perform(post("/auth/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(
                                Map.of("email", "kafka-down@pulse.dev", "password", "secret-password"))))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String userId = objectMapper.readTree(body).get("userId").asText();

        mockMvc.perform(post("/users/" + userId + "/metrics")
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"metrics\":[{\"name\":\"cpu\",\"value\":1,"
                                + "\"timestamp\":\"2026-11-06T10:00:00Z\"}]}"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value(503));

        // Незавершённый шаг приёма снят: клиент может повторить запрос после восстановления Kafka.
        Integer stuck = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM metric.ingest_idempotency WHERE user_id = ?", Integer.class,
                UUID.fromString(userId));
        assertThat(stuck).isZero();
    }
}
