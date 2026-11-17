package com.pulse.metric;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Fail-fast чтения (находка F5): при мёртвом ClickHouse GET истории отдаёт 503 быстро —
 * TimeLimiter обрывает зависший драйвер, circuit breaker далее не пропускает вызовы.
 * Чтение инцидентов из PostgreSQL (другая цепь) продолжает работать.
 * Контекст отдельный: ускоренные таймауты breaker/limiter.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "resilience4j.circuitbreaker.instances.chRead.sliding-window-size=3",
        "resilience4j.circuitbreaker.instances.chRead.minimum-number-of-calls=1",
        "resilience4j.circuitbreaker.instances.chRead.failure-rate-threshold=50",
        "resilience4j.circuitbreaker.instances.chRead.wait-duration-in-open-state=60s",
        "resilience4j.timelimiter.instances.chRead.timeout-duration=2s",
        "resilience4j.timelimiter.instances.pgRead.timeout-duration=2s"
})
@AutoConfigureMockMvc
@EmbeddedKafka(topics = "metrics", count = 1)
@Testcontainers
class ReadFailFastIntegrationTest {

    private static final String CH_USER = "pulse";
    private static final String CH_PASSWORD = "ff-test-pw";

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    static final GenericContainer<?> CLICKHOUSE =
            new GenericContainer<>(DockerImageName.parse("clickhouse/clickhouse-server:24.8"))
                    .withEnv("CLICKHOUSE_USER", CH_USER)
                    .withEnv("CLICKHOUSE_PASSWORD", CH_PASSWORD)
                    .withExposedPorts(8123);

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.clickhouse.url", () -> "jdbc:clickhouse://" + CLICKHOUSE.getHost()
                + ":" + CLICKHOUSE.getMappedPort(8123) + "/default");
        registry.add("spring.clickhouse.username", () -> CH_USER);
        registry.add("spring.clickhouse.password", () -> CH_PASSWORD);
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeAll
    static void startClickHouseAndCreateSchema() throws Exception {
        CLICKHOUSE.start();
        try (Connection connection = DriverManager.getConnection("jdbc:clickhouse://"
                        + CLICKHOUSE.getHost() + ":" + CLICKHOUSE.getMappedPort(8123)
                        + "/default", CH_USER, CH_PASSWORD);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS pulse");
            statement.execute("CREATE TABLE IF NOT EXISTS pulse.metric_points ("
                    + "userId UUID, metricName String, value Float64,"
                    + " timestamp DateTime64(3, 'UTC'), eventId UUID,"
                    + " insertedAt DateTime64(3, 'UTC') DEFAULT now64(3))"
                    + " ENGINE = ReplacingMergeTree(insertedAt)"
                    + " ORDER BY (userId, metricName, timestamp, eventId)");
        }
    }

    @Test
    void shouldReturn503FastAndKeepIncidentRead_whenClickHouseDown() throws Exception {
        String body = mockMvc.perform(post("/auth/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Map.of(
                                "email", "ff@pulse.dev", "password", "ff-password"))))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        UUID userId = UUID.fromString(objectMapper.readTree(body).get("userId").asText());
        HttpHeaders basic = new HttpHeaders();
        basic.set(HttpHeaders.AUTHORIZATION, "Basic " + Base64.getEncoder().encodeToString(
                (userId + ":ff-password").getBytes(StandardCharsets.UTF_8)));
        givenIncident(userId);

        CLICKHOUSE.stop();

        // TimeLimiter (2 c) + min-calls=1: первый же запрос открывается в 503 RFC 7807,
        // далее breaker удерживает цепь открытой (open 60 c) — все ответы быстрые.
        for (int attempt = 0; attempt < 4; attempt++) {
            long start = System.nanoTime();
            read(get("/users/{id}/metrics", userId).headers(basic))
                    .andExpect(status().isServiceUnavailable())
                    .andExpect(content().contentTypeCompatibleWith(
                            MediaType.APPLICATION_PROBLEM_JSON));
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            assertThat(elapsedMs).as("attempt %d must fast-fail, not hang on driver", attempt)
                    .isLessThan(2000);
        }

        // PostgreSQL жив: чтение инцидентов другой цепью не задето.
        read(get("/users/{id}/incidents", userId).headers(basic))
                .andExpect(status().isOk());
    }

    /** Read-endpoints асинхронные (TimeLimiter) — диспатч завершает обработку. */
    private ResultActions read(MockHttpServletRequestBuilder request) throws Exception {
        ResultActions actions = mockMvc.perform(request);
        MvcResult result = actions.andReturn();
        if (result.getRequest().isAsyncStarted()) {
            actions = mockMvc.perform(asyncDispatch(result));
        }
        return actions;
    }

    /** Инциденты читаются из PostgreSQL — таблица создаётся тем же DDL, что и в микросервисе. */
    private void givenIncident(UUID userId) {
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS metric.incidents ("
                + "id UUID PRIMARY KEY, user_id UUID NOT NULL, rule_id TEXT NOT NULL,"
                + " metric_name TEXT NOT NULL, status TEXT NOT NULL,"
                + " value DOUBLE PRECISION, threshold DOUBLE PRECISION,"
                + " opened_at TIMESTAMPTZ NOT NULL, closed_at TIMESTAMPTZ,"
                + " incident_key TEXT UNIQUE)");
        jdbcTemplate.update("INSERT INTO metric.incidents (id, user_id, rule_id, metric_name,"
                        + " status, value, threshold, opened_at)"
                        + " VALUES (?, ?, 'temperature-high', 'temperature', 'OPEN', 85.0, 80.0, ?)",
                UUID.randomUUID(), userId, Timestamp.from(Instant.parse("2026-11-06T10:00:01Z")));
    }
}
