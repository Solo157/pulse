package com.pulse.storage.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.description;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pulse.contracts.common.MetricPoint;
import com.pulse.contracts.event.MetricEvent;
import com.pulse.contracts.event.SchemaVersions;
import com.pulse.storage.service.ClickHousePointsWriter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Сквозные гистограммы потребителя storage: возраст пакета на входе и задержка
 * до видимости в ClickHouse измеряются от acceptedAt (изоляция: замоканный writer,
 * фиксированные часы, реестр в памяти).
 */
@ExtendWith(MockitoExtension.class)
class MetricEventConsumerListenerTest {

    private static final UUID USER = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final Instant NOW = Instant.parse("2026-11-09T10:00:00Z");

    @Mock
    private ClickHousePointsWriter writer;

    @Test
    void shouldRecordAgeAndVisibleLatency_whenInsertSucceeds() {
        // Arrange
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricEventConsumerListener listener = listenerWith(registry, Clock.fixed(NOW, ZoneOffset.UTC));
        String payload = payload(Instant.parse("2026-11-09T09:59:55Z"));

        // Act
        listener.onBatch(List.of(record(payload)));

        // Assert
        assertThat(registry.timer("pulse.event.age", "consumer", "storage-group").totalTime(TimeUnit.SECONDS))
                .as("возраст пакета на входе = acceptedAt -> now")
                .isEqualTo(5.0);
        assertThat(registry.timer("pulse.e2e.clickhouse.visible").totalTime(TimeUnit.SECONDS))
                .as("видимость в ClickHouse отсчитывается после вставки той же точки")
                .isEqualTo(5.0);
        verify(writer, description("точки записаны до фиксации видимости"))
                .insertPoints(USER.toString(), "event-1", List.of(
                        new MetricPoint("cpu", 1.0, NOW.minusSeconds(5))));
    }

    @Test
    void shouldSkipLatencyMetrics_whenAcceptedAtMissing() {
        // Arrange
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricEventConsumerListener listener = listenerWith(registry, Clock.fixed(NOW, ZoneOffset.UTC));
        String payload = payload(null);

        // Act
        listener.onBatch(List.of(record(payload)));

        // Assert
        assertThat(registry.timer("pulse.event.age", "consumer", "storage-group").count())
                .as("событие без acceptedAt не добавляет наблюдений в гистограмму")
                .isZero();
        assertThat(registry.timer("pulse.e2e.clickhouse.visible").count())
                .as("и в задержку видимости тоже")
                .isZero();
    }

    private MetricEventConsumerListener listenerWith(SimpleMeterRegistry registry, Clock clock) {
        return new MetricEventConsumerListener(writer, objectMapper(), registry, clock);
    }

    private static ObjectMapper objectMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }

    private static String payload(Instant acceptedAt) {
        try {
            return objectMapper().writeValueAsString(new MetricEvent("event-1", USER.toString(),
                    SchemaVersions.CURRENT, acceptedAt, "req-1",
                    List.of(new MetricPoint("cpu", 1.0, acceptedAt == null ? NOW : acceptedAt))));
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static ConsumerRecord<String, String> record(String payload) {
        return new ConsumerRecord<>("metrics", 0, 0L, USER.toString(), payload);
    }
}
