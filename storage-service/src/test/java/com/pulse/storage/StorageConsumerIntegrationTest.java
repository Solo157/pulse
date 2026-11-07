package com.pulse.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulse.contracts.common.MetricPoint;
import com.pulse.contracts.event.MetricEvent;
import com.pulse.contracts.event.SchemaVersions;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;


/**
 * Интеграционный тест storage-group (раздел 16): реальные Kafka и ClickHouse.
 * Проверяет пакетную запись и дедупликацию повторной доставки события (п. 8).
 */
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(topics = "metrics", count = 1)
@Testcontainers
class StorageConsumerIntegrationTest {

    private static final int HTTP_PORT = 8123;
    private static final String DB_USER = "chuser";
    private static final String DB_PASSWORD = "chpass";

    // 24.8 задаёт пароль default-пользователя из образа, а JdbcDatabaseContainer
    // прокси-класс проверяется своими «встроенными» кредами — берём GenericContainer.
    @Container
    static final GenericContainer<?> CLICKHOUSE =
            new GenericContainer<>(DockerImageName.parse("clickhouse/clickhouse-server:24.8"))
                    .withEnv("CLICKHOUSE_USER", DB_USER)
                    .withEnv("CLICKHOUSE_PASSWORD", DB_PASSWORD)
                    .withExposedPorts(HTTP_PORT)
                    .waitingFor(Wait.forHttp("/ping").forStatusCode(200));

    @DynamicPropertySource
    static void configure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> "jdbc:clickhouse://"
                + CLICKHOUSE.getHost() + ":" + CLICKHOUSE.getMappedPort(HTTP_PORT) + "/default");
        registry.add("spring.datasource.username", () -> DB_USER);
        registry.add("spring.datasource.password", () -> DB_PASSWORD);
        // Таблицы создаёт schema.sql в схемах pulse.*; читает тест тоже pulse.metric_points.
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void пакетТочекПишетсяВClickHouseИдемпотентноПриПовторе() throws Exception {
        UUID userId = UUID.randomUUID();
        String eventId = UUID.randomUUID().toString();
        Instant sampleTime = Instant.parse("2026-11-06T10:00:00Z");
        MetricEvent event = new MetricEvent(eventId, userId.toString(), SchemaVersions.CURRENT,
                Instant.now(), "test-request", List.of(
                new MetricPoint("cpu_usage", 42.5, sampleTime),
                new MetricPoint("mem_usage", 17.0, sampleTime)));
        String payload = objectMapper.writeValueAsString(event);

        // Первая доставка.
        kafkaTemplate.send("metrics", userId.toString(), payload).get(10, java.util.concurrent.TimeUnit.SECONDS);
        awaitRows(userId, 2);

        // Повторная доставка того же события (моделирует redelivery Kafka).
        kafkaTemplate.send("metrics", userId.toString(), payload).get(10, java.util.concurrent.TimeUnit.SECONDS);
        awaitRows(userId, 4); // физически вставятся дубли до слияния parts

        // Дедупликация движка: после слияния частей остаётся ровно по одной точке на (userId, metric, ts, eventId).
        jdbcTemplate.execute("OPTIMIZE TABLE pulse.metric_points FINAL");
        Integer distinctRows = jdbcTemplate.queryForObject(
                "SELECT count() FROM pulse.metric_points FINAL WHERE userId = ?",
                Integer.class, userId.toString());
        assertThat(distinctRows).isEqualTo(2);
    }

    private void awaitRows(UUID userId, int expected) {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            Integer rows = jdbcTemplate.queryForObject(
                    "SELECT count() FROM pulse.metric_points WHERE userId = ?",
                    Integer.class, userId.toString());
            if (rows != null && rows >= expected) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
