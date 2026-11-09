package com.pulse.processing.listener;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pulse.contracts.common.MetricPoint;
import com.pulse.contracts.event.MetricEvent;
import com.pulse.contracts.event.SchemaVersions;
import com.pulse.processing.service.IncidentEvaluator;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Возраст пакета метрик на входе группы processing-metrics: гистограмма
 * {@code pulse.event.age} отсчитывается от acceptedAt (изоляция: замоканный
 * оценщик, фиксированные часы, реестр в памяти).
 */
@ExtendWith(MockitoExtension.class)
class MetricIncidentListenerTest {

    private static final UUID USER = UUID.fromString("77777777-7777-7777-7777-777777777777");
    private static final Instant NOW = Instant.parse("2026-11-09T10:00:00Z");

    @Mock
    private IncidentEvaluator evaluator;

    @Test
    void shouldRecordEventAge_whenAcceptedAtPresent() {
        // Arrange
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricIncidentListener listener = new MetricIncidentListener(evaluator,
                new ObjectMapper().registerModule(new JavaTimeModule()), registry,
                Clock.fixed(NOW, ZoneOffset.UTC));
        String payload = payload(Instant.parse("2026-11-09T09:59:57Z"));

        // Act
        listener.onMetricEvent(payload);

        // Assert
        assertThat(registry.timer("pulse.event.age", "consumer", "processing-metrics").totalTime(TimeUnit.SECONDS))
                .as("возраст пакета от acceptedAt до момента обработки")
                .isEqualTo(3.0);
    }

    @Test
    void shouldSkipEventAge_whenAcceptedAtMissing() {
        // Arrange
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricIncidentListener listener = new MetricIncidentListener(evaluator,
                new ObjectMapper().registerModule(new JavaTimeModule()), registry,
                Clock.fixed(NOW, ZoneOffset.UTC));

        // Act
        listener.onMetricEvent(payload(null));

        // Assert
        assertThat(registry.timer("pulse.event.age", "consumer", "processing-metrics").count())
                .as("событие без acceptedAt не добавляет наблюдений в гистограмму")
                .isZero();
    }

    private static String payload(Instant acceptedAt) {
        try {
            return new ObjectMapper().registerModule(new JavaTimeModule()).writeValueAsString(
                    new MetricEvent("event-1", USER.toString(), SchemaVersions.CURRENT, acceptedAt,
                            "req-1", List.of(new MetricPoint("cpu", 1.0, acceptedAt == null
                                    ? NOW : acceptedAt))));
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }
}
