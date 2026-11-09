package com.pulse.processing.listener;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pulse.contracts.event.IncidentDetected;
import com.pulse.contracts.event.SchemaVersions;
import com.pulse.processing.state.IncidentRowRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Возраст события инцидента на входе группы metric-group: гистограмма
 * {@code pulse.event.age} отсчитывается от createdAt (изоляция: замоканный
 * репозиторий, фиксированные часы, реестр в памяти).
 */
@ExtendWith(MockitoExtension.class)
class IncidentPersistListenerTest {

    private static final UUID USER = UUID.fromString("88888888-8888-8888-8888-888888888888");
    private static final UUID EVENT = UUID.fromString("99999999-9999-9999-9999-999999999999");
    private static final Instant NOW = Instant.parse("2026-11-09T10:00:00Z");

    @Mock
    private IncidentRowRepository rowRepository;

    @Test
    void shouldRecordEventAge_whenIncidentPersisted() {
        // Arrange
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        IncidentPersistListener listener = new IncidentPersistListener(rowRepository,
                new ObjectMapper().registerModule(new JavaTimeModule()), registry,
                Clock.fixed(NOW, ZoneOffset.UTC));
        String payload = payload(NOW.minusSeconds(6));

        // Act
        listener.onIncidentDetected(payload);

        // Assert
        assertThat(registry.timer("pulse.event.age", "consumer", "metric-group").totalTime(TimeUnit.SECONDS))
                .as("возраст события от публикации инцидента до обработки")
                .isEqualTo(6.0);
    }

    private static String payload(Instant createdAt) {
        try {
            return new ObjectMapper().registerModule(new JavaTimeModule()).writeValueAsString(
                    new IncidentDetected(EVENT.toString(), USER.toString(), "rule-1",
                            USER + ":rule-1", false, "cpu", 80.0, 90.0, createdAt,
                            createdAt.minusSeconds(2), SchemaVersions.CURRENT, "req-1"));
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }
}
