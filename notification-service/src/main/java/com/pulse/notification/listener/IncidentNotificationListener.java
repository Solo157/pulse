package com.pulse.notification.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulse.contracts.event.IncidentDetected;
import com.pulse.notification.service.NotificationService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Группа {@code notification-group}: читает топик {@code incidents} и запускает
 * создание/доставку уведомления (FR-5). Ошибки обработки (например, БД недоступна)
 * повторяются error-handler'ом без коммита offset; сбой самого канала доставки
 * терминален для уведомления и фиксируется статусом FAILED.
 *
 * <p>Гистограмма {@code pulse.event.age} — возраст события инцидента на входе
 * группы от момента его публикации (createdAt).</p>
 */
@Component
public class IncidentNotificationListener {

    private final NotificationService notificationService;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final Timer eventAgeTimer;

    public IncidentNotificationListener(NotificationService notificationService,
            ObjectMapper objectMapper, MeterRegistry meterRegistry, Clock clock) {
        this.notificationService = notificationService;
        this.objectMapper = objectMapper;
        this.clock = clock;
        this.eventAgeTimer = meterRegistry.timer("pulse.event.age", "consumer", "notification-group");
    }

    @KafkaListener(id = "notification-incidents", topics = "${pulse.incidents-topic}",
            groupId = "notification-group")
    public void onIncidentDetected(String payload) {
        try {
            IncidentDetected event = objectMapper.readValue(payload, IncidentDetected.class);
            Instant createdAt = event.createdAt();
            if (createdAt != null) {
                eventAgeTimer.record(Duration.between(createdAt, Instant.now(clock)));
            }
            notificationService.onIncident(event);
        } catch (Exception failure) {
            throw new IllegalArgumentException("Не обработано событие IncidentDetected: " + payload, failure);
        }
    }
}
