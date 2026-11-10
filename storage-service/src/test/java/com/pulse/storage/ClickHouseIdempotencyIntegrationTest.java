package com.pulse.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pulse.contracts.common.MetricPoint;
import com.pulse.storage.service.ClickHousePointsWriter;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Идемпотентность и частичные сбои записи в ClickHouse (P1-5): гарантия
 * описывается как at-least-once + дедупликация по идентичности события на уровне
 * движка (ReplacingMergeTree) и чтения (FINAL), а не exactly-once.
 * Повтор события через Kafka покрыт {@link StorageConsumerIntegrationTest};
 * здесь — уровни писателя: ошибочный батч, повтор после неопределённого
 * результата вставки, корректность sorting key.
 */
@SpringBootTest(properties = {
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.bootstrap-servers=localhost:19092"
})
@Testcontainers
@TestMethodOrder(OrderAnnotation.class)
class ClickHouseIdempotencyIntegrationTest {

    private static final int HTTP_PORT = 8123;
    private static final String DB_USER = "chuser";
    private static final String DB_PASSWORD = "chpass";

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
    }

    @Autowired
    private ClickHousePointsWriter writer;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    @Order(1)
    void should_whenBatchFailsPartial_then_noPartialRowsPersisted() {
        UUID userId = UUID.randomUUID();
        Instant ts = Instant.parse("2026-11-09T10:00:00Z");
        writer.insertPoints(userId.toString(), UUID.randomUUID().toString(), List.of(
                new MetricPoint("cpu_usage", 1.0, ts),
                new MetricPoint("mem_usage", 2.0, ts)));

        String badUserId = "это-not-a-uuid";
        int beforeFailure = countTable();
        assertThatThrownBy(() -> writer.insertPoints(badUserId, UUID.randomUUID().toString(), List.of(
                new MetricPoint("cpu_usage", 3.0, ts),
                new MetricPoint("disk_usage", 4.0, ts),
                new MetricPoint("net_usage", 5.0, ts))))
                .isInstanceOf(RuntimeException.class);

        assertThat(countGood(userId)).isEqualTo(2);
        // Неопределённый/ошибочный батч не оставил «половин» — таблица не выросла.
        assertThat(countTable()).isEqualTo(beforeFailure);
    }

    @Test
    @Order(2)
    void should_whenRetryAfterUncertainInsert_then_finalSelectReturnsUniquePoints() {
        UUID userId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        Instant ts = Instant.parse("2026-11-09T11:00:00Z");
        List<MetricPoint> points = List.of(
                new MetricPoint("cpu_usage", 10.0, ts),
                new MetricPoint("cpu_usage", 11.0, ts.plusSeconds(1)),
                new MetricPoint("mem_usage", 20.0, ts));

        // Вставка выполнилась, но подтверждение потеряно («неопределённый результат»).
        insertRaw(userId, eventId, points);
        // Потребитель повторяет то же событие — писатель вставляет те же точки второй раз.
        writer.insertPoints(userId.toString(), eventId.toString(), points);

        assertThat(countRaw(userId.toString())).isEqualTo(6);
        jdbcTemplate.execute("OPTIMIZE TABLE pulse.metric_points FINAL");
        assertThat(countGood(userId)).isEqualTo(3);
        assertThat(duplicateKeys(userId)).isZero();
    }

    @Test
    @Order(3)
    void should_whenSamePointTwiceInOneEvent_then_mergedBySortingKey() {
        UUID userId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        Instant ts = Instant.parse("2026-11-09T12:00:00Z");
        MetricPoint repeated = new MetricPoint("cpu_usage", 42.0, ts);
        writer.insertPoints(userId.toString(), eventId.toString(),
                List.of(repeated, repeated, new MetricPoint("mem_usage", 1.0, ts)));

        jdbcTemplate.execute("OPTIMIZE TABLE pulse.metric_points FINAL");
        assertThat(countGood(userId)).isEqualTo(2);
        assertThat(duplicateKeys(userId)).isZero();
    }

    private void insertRaw(UUID userId, UUID eventId, List<MetricPoint> points) {
        for (MetricPoint point : points) {
            jdbcTemplate.update("""
                    INSERT INTO pulse.metric_points (userId, metricName, value, timestamp, eventId)
                    VALUES (?, ?, ?, ?, ?)
                    """, userId.toString(), point.name(), point.value(),
                    Timestamp.from(point.timestamp()), eventId.toString());
        }
    }

    private int countTable() {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT count() FROM pulse.metric_points", Integer.class);
        return count == null ? 0 : count;
    }

    private int countRaw(String userId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT count() FROM pulse.metric_points WHERE userId = ?",
                Integer.class, userId);
        return count == null ? 0 : count;
    }

    private int countGood(UUID userId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT count() FROM pulse.metric_points FINAL WHERE userId = ?",
                Integer.class, userId.toString());
        return count == null ? 0 : count;
    }

    /** Точки с одинаковым ключом сортировки должны сливаться — иначе ключ некорректен. */
    private int duplicateKeys(UUID userId) {
        Integer count = jdbcTemplate.queryForObject("""
                SELECT count() FROM (
                    SELECT userId, metricName, timestamp, eventId
                    FROM pulse.metric_points FINAL
                    WHERE userId = ?
                    GROUP BY userId, metricName, timestamp, eventId
                    HAVING count() > 1
                )
                """, Integer.class, userId.toString());
        return count == null ? 0 : count;
    }
}
