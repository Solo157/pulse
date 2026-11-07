package com.pulse.notification.service;

import com.pulse.contracts.event.IncidentDetected;
import com.pulse.notification.config.NotificationProperties;
import com.pulse.notification.repository.NotificationRepository;
import com.pulse.notification.repository.NotificationStatus;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Сохранение уведомления и демонстрационная доставка (FR-5, раздел 6.5).
 *
 * <p>Идемпотентность: строка уведомлений регистрируется по {@code source_event_id}
 * (eventId события Kafka) — повторная доставка события не создаёт второе уведомление
 * и не повторяет доставку. Демонстрационный канал — HTTP: доставка выполняется
 * синхронно с ограниченным числом попыток и паузами между ними; терминальный
 * статус ({@code SENT}/{@code FAILED}) всегда зафиксирован в БД, это и есть
 * проверяемый этап доставки.</p>
 */
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final NotificationRepository repository;
    private final NotificationDeliveryClient deliveryClient;
    private final NotificationProperties properties;
    private final MeterRegistry meterRegistry;

    public NotificationService(NotificationRepository repository,
            NotificationDeliveryClient deliveryClient, NotificationProperties properties,
            MeterRegistry meterRegistry) {
        this.repository = repository;
        this.deliveryClient = deliveryClient;
        this.properties = properties;
        this.meterRegistry = meterRegistry;
    }

    /** Обрабатывает событие инцидента: только открытие порождает уведомление. */
    public void onIncident(IncidentDetected event) {
        if (event.close()) {
            log.debug("Событие закрытия {} — уведомление не требуется", event.incidentKey());
            return;
        }
        UUID userId = UUID.fromString(event.userId());
        String destination = repository.findEmail(userId).orElse(event.userId() + "@pulse.dev");
        if (!repository.claim(event.eventId(), userId, event.incidentKey(), destination)) {
            meterRegistry.counter("pulse.notifications", "status", "duplicate").increment();
            return;
        }

        Map<String, Object> payload = Map.of(
                "notificationId", event.eventId(),
                "userId", event.userId(),
                "incidentKey", event.incidentKey(),
                "destination", destination,
                "text", "Инцидент %s: правило %s, значение %s > %.1f"
                        .formatted(event.incidentKey(), event.ruleId(), event.value(), event.threshold()));

        for (int attempt = 1; attempt <= properties.deliveryMaxAttempts(); attempt++) {
            if (deliveryClient.deliver(payload)) {
                repository.finish(event.eventId(), NotificationStatus.SENT, attempt, Instant.now());
                meterRegistry.counter("pulse.notifications", "status", "sent").increment();
                log.info("Уведомление {} доставлено (попытка {})", event.eventId(), attempt);
                return;
            }
            if (attempt < properties.deliveryMaxAttempts()) {
                sleepQuietly();
            }
        }

        repository.finish(event.eventId(), NotificationStatus.FAILED,
                properties.deliveryMaxAttempts(), null);
        meterRegistry.counter("pulse.notifications", "status", "failed").increment();
        log.error("Уведомление {}: канал недоступен, попытки исчерпаны ({})",
                event.eventId(), properties.deliveryMaxAttempts());
    }

    private void sleepQuietly() {
        try {
            Thread.sleep(properties.deliveryRetryDelay().toMillis());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
