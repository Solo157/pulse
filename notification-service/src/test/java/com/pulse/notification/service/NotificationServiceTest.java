package com.pulse.notification.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.pulse.contracts.event.IncidentDetected;
import com.pulse.contracts.event.SchemaVersions;
import com.pulse.notification.config.NotificationProperties;
import com.pulse.notification.repository.NotificationRepository;
import com.pulse.notification.repository.NotificationStatus;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Гистограмма «incident published → notification SENT»: от createdAt события
 * до подтверждения демонстрационного канала (изоляция: замоканные репозиторий
 * и канал, фиксированные часы, реестр в памяти).
 */
@ExtendWith(MockitoExtension.class)
class NotificationServiceTest {

    private static final UUID USER = UUID.fromString("55555555-5555-5555-5555-555555555555");
    private static final UUID EVENT = UUID.fromString("66666666-6666-6666-6666-666666666666");
    private static final Instant NOW = Instant.parse("2026-11-09T10:00:00Z");

    @Mock
    private NotificationRepository repository;

    @Mock
    private NotificationDeliveryClient deliveryClient;

    @Test
    void shouldRecordDeliveryLatency_whenChannelConfirms() {
        // Arrange
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        NotificationService service = serviceWith(registry, Clock.fixed(NOW, ZoneOffset.UTC));
        when(repository.findEmail(USER)).thenReturn(Optional.of("owner@pulse.dev"));
        when(repository.claim(eq(EVENT.toString()), eq(USER), any(), any())).thenReturn(true);
        when(deliveryClient.deliver(any())).thenReturn(true);

        // Act
        service.onIncident(incident(NOW.minusSeconds(4)));

        // Assert
        assertThat(registry.timer("pulse.e2e.notification.sent").totalTime(TimeUnit.SECONDS))
                .as("от публикации инцидента до SENT — 4 секунды")
                .isEqualTo(4.0);
    }

    @Test
    void shouldSkipLatency_whenIncidentCreatedAtMissing() {
        // Arrange
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        NotificationService service = serviceWith(registry, Clock.fixed(NOW, ZoneOffset.UTC));
        when(repository.findEmail(USER)).thenReturn(Optional.of("owner@pulse.dev"));
        when(repository.claim(eq(EVENT.toString()), eq(USER), any(), any())).thenReturn(true);
        when(deliveryClient.deliver(any())).thenReturn(true);

        // Act
        service.onIncident(incident(null));

        // Assert
        assertThat(registry.timer("pulse.e2e.notification.sent").count())
                .as("событие без createdAt не добавляет наблюдений в гистограмму")
                .isZero();
    }

    private NotificationService serviceWith(SimpleMeterRegistry registry, Clock clock) {
        NotificationProperties properties = new NotificationProperties("incidents", "http://demo",
                Duration.ofSeconds(2), 3, Duration.ZERO);
        return new NotificationService(repository, deliveryClient, properties, registry, clock);
    }

    private static IncidentDetected incident(Instant createdAt) {
        return new IncidentDetected(EVENT.toString(), USER.toString(), "rule-1",
                USER + ":rule-1", false, "cpu", 80.0, 90.0, createdAt,
                NOW.minusSeconds(10), SchemaVersions.CURRENT, "req-1");
    }
}
