package com.pulse.metric;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
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
 * Read API (FR-4, Этап 5): клиент читает только свои данные.
 * Метрики — из реального ClickHouse, инциденты — из PostgreSQL (сидируются напрямую,
 * их пишет ProcessingService). Аутентификация — Basic (login = userId).
 * Read-endpoints асинхронные (TimeLimiter) — проверки идут через {@link #read}.
 */
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@AutoConfigureMockMvc
@EmbeddedKafka(topics = "metrics", count = 1)
@Testcontainers
class ReadApiIntegrationTest {

    private static final String CH_USER = "pulse";
    private static final String CH_PASSWORD = "read-test-pw";

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @Container
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
    static void createClickHouseSchema() throws Exception {
        try (Connection connection = chConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS pulse");
            statement.execute("CREATE TABLE IF NOT EXISTS pulse.metric_points ("
                    + "userId UUID, metricName String, value Float64,"
                    + " timestamp DateTime64(3, 'UTC'), eventId UUID,"
                    + " insertedAt DateTime64(3, 'UTC') DEFAULT now64(3))"
                    + " ENGINE = ReplacingMergeTree(insertedAt)"
                    + " PARTITION BY toYYYYMMDD(timestamp)"
                    + " ORDER BY (userId, metricName, timestamp, eventId)");
        }
    }

    @Test
    void владелецЧитаетСвоиМетрикиИИнциденты() throws Exception {
        Owner owner = register("owner@pulse.dev");
        givenMetricPoint(owner.id(), "temperature", 85.0, Instant.parse("2026-11-06T10:00:00Z"));
        givenMetricPoint(owner.id(), "humidity", 40.0, Instant.parse("2026-11-06T11:00:00Z"));
        givenIncident(owner.id(), "OPEN");

        // Фильтры metric/from/limit отрабатывают так же, как без них.
        read(get("/users/{id}/metrics", owner.id()).headers(basic(owner))
                        .param("metric", "temperature")
                        .param("from", "2026-11-06T00:00:00Z")
                        .param("limit", "5"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].name").value("temperature"))
                .andExpect(jsonPath("$[0].value").value(85.0));

        read(get("/users/{id}/incidents", owner.id()).headers(basic(owner)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].ruleId").value("temperature-high"))
                .andExpect(jsonPath("$[0].status").value("OPEN"));
    }

    @Test
    void чужиеДанныеЗапрещены() throws Exception {
        Owner owner = register("real-owner@pulse.dev");
        Owner stranger = register("stranger@pulse.dev");
        givenMetricPoint(owner.id(), "temperature", 42.0, Instant.parse("2026-11-06T10:00:00Z"));

        read(get("/users/{id}/metrics", owner.id()).headers(basic(stranger)))
                .andExpect(status().isForbidden())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
    }

    @Test
    void безУчётныхДанныхИлиСНевернымПаролемСорокОдин() throws Exception {
        Owner owner = register("noauth@pulse.dev");

        read(get("/users/{id}/metrics", owner.id()))
                .andExpect(status().isUnauthorized())
                .andExpect(header().string(HttpHeaders.WWW_AUTHENTICATE,
                        org.hamcrest.Matchers.containsString("Basic")));

        read(get("/users/{id}/metrics", owner.id())
                        .headers(basic(owner.id(), "wrong-password")))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void ingestБезАутентификацииРаботаетКакПрежний() throws Exception {
        Owner owner = register("ingest-public@pulse.dev");
        mockMvc.perform(post("/users/{id}/metrics", owner.id())
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .content(objectMapper.writeValueAsString(Map.of("metrics", List.of(
                                Map.of("name", "temperature", "value", 20.0,
                                        "timestamp", "2026-11-06T10:00:00Z"))))))
                .andExpect(status().isAccepted());
    }

    /**
     * Асинхронно-aware GET: читающий endpoint отдаёт {@code CompletableFuture} (TimeLimiter),
     * поэтому при старте async-контекста повторяем тот же запрос с той же учётной строкой —
     * этот диспатч завершает обработку и несёт итоговый статус.
     */
    private ResultActions read(MockHttpServletRequestBuilder request) throws Exception {
        ResultActions actions = mockMvc.perform(request);
        MvcResult result = actions.andReturn();
        if (result.getRequest().isAsyncStarted()) {
            actions = mockMvc.perform(asyncDispatch(result));
        }
        return actions;
    }

    private Owner register(String email) throws Exception {
        String body = mockMvc.perform(post("/auth/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(
                                Map.of("email", email, "password", "owner-password"))))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return new Owner(UUID.fromString(objectMapper.readTree(body).get("userId").asText()),
                "owner-password");
    }

    private HttpHeaders basic(Owner owner) {
        return basic(owner.id(), owner.password());
    }

    private HttpHeaders basic(UUID userId, String password) {
        HttpHeaders headers = new HttpHeaders();
        headers.set(HttpHeaders.AUTHORIZATION, "Basic " + Base64.getEncoder().encodeToString(
                (userId + ":" + password).getBytes(StandardCharsets.UTF_8)));
        return headers;
    }

    private static java.sql.Connection chConnection() throws Exception {
        return DriverManager.getConnection("jdbc:clickhouse://" + CLICKHOUSE.getHost() + ":"
                + CLICKHOUSE.getMappedPort(8123) + "/default", CH_USER, CH_PASSWORD);
    }

    /** Прямая вставка в тот же ClickHouse, куда пишет storage-service. */
    private void givenMetricPoint(UUID userId, String name, double value, Instant timestamp)
            throws Exception {
        try (Connection connection = chConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "INSERT INTO pulse.metric_points (userId, metricName, value, timestamp, eventId)"
                             + " VALUES (?, ?, ?, ?, ?)")) {
            statement.setString(1, userId.toString());
            statement.setString(2, name);
            statement.setDouble(3, value);
            statement.setTimestamp(4, Timestamp.from(timestamp));
            statement.setString(5, UUID.randomUUID().toString());
            statement.executeUpdate();
        }
    }

    private void givenIncident(UUID userId, String status) {
        // Инциденты ведёт ProcessingService; для теста достаточно той же структуры.
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS metric.incidents ("
                + "id UUID PRIMARY KEY, user_id UUID NOT NULL, rule_id TEXT NOT NULL,"
                + " metric_name TEXT NOT NULL, status TEXT NOT NULL,"
                + " value DOUBLE PRECISION, threshold DOUBLE PRECISION,"
                + " opened_at TIMESTAMPTZ NOT NULL, closed_at TIMESTAMPTZ,"
                + " incident_key TEXT UNIQUE)");
        jdbcTemplate.update("INSERT INTO metric.incidents (id, user_id, rule_id, metric_name,"
                        + " status, value, threshold, opened_at)"
                        + " VALUES (?, ?, 'temperature-high', 'temperature', ?, 85.0, 80.0, ?)",
                UUID.randomUUID(), userId, status,
                Timestamp.from(Instant.parse("2026-11-06T10:00:01Z")));
    }

    private record Owner(UUID id, String password) {
    }
}
