package com.pulse.processing.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pulse.contracts.common.MetricPoint;
import com.pulse.contracts.event.MetricEvent;
import com.pulse.contracts.event.SchemaVersions;
import com.pulse.processing.service.IncidentEvaluator;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Группа processing-metrics разбирает MetricEvent и передаёт его оценщику;
 * неразобранное сообщение — сигнал для повтора error-handler'ом (изоляция:
 * замоканный оценщик).
 */
@ExtendWith(MockitoExtension.class)
class MetricIncidentListenerTest {

    private static final UUID USER = UUID.fromString("77777777-7777-7777-7777-777777777777");
    private static final Instant NOW = Instant.parse("2026-11-09T10:00:00Z");

    @Mock
    private IncidentEvaluator evaluator;

    @Test
    void shouldDelegateParsedEvent_whenPayloadValid() {
        // Arrange
        MetricIncidentListener listener = new MetricIncidentListener(evaluator, objectMapper());

        // Act
        listener.onMetricEvent(payload());

        // Assert
        ArgumentCaptor<MetricEvent> captor = ArgumentCaptor.forClass(MetricEvent.class);
        verify(evaluator).process(captor.capture());
        assertThat(captor.getValue().eventId()).isEqualTo("event-1");
        assertThat(captor.getValue().userId()).isEqualTo(USER.toString());
    }

    @Test
    void shouldFailForRedelivery_whenPayloadUnreadable() {
        // Arrange
        MetricIncidentListener listener = new MetricIncidentListener(evaluator, objectMapper());

        // Act & Assert
        assertThatThrownBy(() -> listener.onMetricEvent("{битое"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static ObjectMapper objectMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }

    private static String payload() {
        try {
            return objectMapper().writeValueAsString(new MetricEvent("event-1", USER.toString(),
                    SchemaVersions.CURRENT, NOW, "req-1",
                    List.of(new MetricPoint("cpu", 1.0, NOW))));
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }
}
