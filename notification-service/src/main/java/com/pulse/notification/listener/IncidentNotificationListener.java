package com.pulse.notification.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulse.contracts.event.IncidentDetected;
import com.pulse.notification.service.NotificationService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Группа {@code notification-group}: читает топик {@code incidents} и запускает
 * создание/доставку уведомления (FR-5). Ошибки обработки (например, БД недоступна)
 * повторяются error-handler'ом без коммита offset; сбой самого канала доставки
 * терминален для уведомления и фиксируется статусом FAILED.
 */
@Component
public class IncidentNotificationListener {

    private final NotificationService notificationService;
    private final ObjectMapper objectMapper;

    public IncidentNotificationListener(NotificationService notificationService,
            ObjectMapper objectMapper) {
        this.notificationService = notificationService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(id = "notification-incidents", topics = "${pulse.incidents-topic}",
            groupId = "notification-group")
    public void onIncidentDetected(String payload) {
        try {
            IncidentDetected event = objectMapper.readValue(payload, IncidentDetected.class);
            notificationService.onIncident(event);
        } catch (Exception failure) {
            throw new IllegalArgumentException("Не обработано событие IncidentDetected: " + payload, failure);
        }
    }
}
