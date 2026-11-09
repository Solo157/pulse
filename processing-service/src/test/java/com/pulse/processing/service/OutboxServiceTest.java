package com.pulse.processing.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pulse.contracts.event.IncidentDetected;
import com.pulse.contracts.event.SchemaVersions;
import com.pulse.processing.state.OutboxRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
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
 * Гистограмма «ingest accepted → incident published»: замерется от sampleTime
 * точки до подтверждения публикации брокером (изоляция: замоканные репозиторий
 * и издатель, фиксированные часы, реестр в памяти).
 */
@ExtendWith(MockitoExtension.class)
class OutboxServiceTest {

    private static final UUID USER = UUID.fromString("22222222-2222-2222-2222-222222222222");
    private static final UUID EVENT = UUID.fromString("44444444-4444-4444-4444-444444444444");
    private static final Instant NOW = Instant.parse("2026-11-09T10:00:00Z");

    @Mock
    private OutboxRepository outbox;

    @Mock
    private IncidentPublisher publisher;

    @Test
    void shouldRecordPublishLatencyFromSampleTime_whenPublished() throws Exception {
        // Arrange
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OutboxService service = new OutboxService(outbox, publisher, objectMapper(), registry,
                Clock.fixed(NOW, ZoneOffset.UTC));
        String payload = payload(Instant.parse("2026-11-09T09:59:53Z"));
        when(outbox.claim(10, Duration.ofMinutes(1)))
                .thenReturn(List.of(new OutboxRepository.Pending(EVENT, USER.toString(), payload, 1)));

        // Act
        int published = service.publishPending(10, Duration.ofMinutes(1), Duration.ofSeconds(1), 4);

        // Assert
        assertThat(published).as("событие опубликовано").isEqualTo(1);
        assertThat(registry.timer("pulse.e2e.incident.published").totalTime(TimeUnit.SECONDS))
                .as("задержка от sampleTime точки до подтверждения брокером")
                .isEqualTo(7.0);
    }

    @Test
    void shouldKeepPublishSuccess_whenPublishedPayloadUnreadable() throws Exception {
        // Arrange
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OutboxService service = new OutboxService(outbox, publisher, objectMapper(), registry,
                Clock.fixed(NOW, ZoneOffset.UTC));
        when(outbox.claim(10, Duration.ofMinutes(1)))
                .thenReturn(List.of(new OutboxRepository.Pending(EVENT, USER.toString(), "{битый", 1)));

        // Act
        int published = service.publishPending(10, Duration.ofMinutes(1), Duration.ofSeconds(1), 4);

        // Assert
        assertThat(published)
                .as("опубликованное событие остаётся успехом, даже если payload не читается для метрики")
                .isEqualTo(1);
        assertThat(registry.timer("pulse.e2e.incident.published").count())
                .as("некорректный payload не добавляет наблюдений в гистограмму")
                .isZero();
    }

    private static ObjectMapper objectMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }

    private static String payload(Instant sampleTime) {
        try {
            return objectMapper().writeValueAsString(new IncidentDetected(EVENT.toString(),
                    USER.toString(), "rule-1", USER + ":rule-1", false, "cpu", 80.0, 90.0,
                    NOW.minusSeconds(1), sampleTime, SchemaVersions.CURRENT, "req-1"));
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }
}
