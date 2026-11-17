package com.pulse.notification.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Настройки уведомлений (префикс {@code pulse}).
 *
 * @param incidentsTopic      топик событий {@code IncidentDetected}
 * @param deliveryUrl         адрес демонстрационного канала доставки (HTTP)
 * @param deliveryTimeout     таймаут одного HTTP-вызова канала
 * @param deliveryMaxAttempts попытки доставки на одно уведомление (retries, раздел 6.5)
 * @param deliveryRetryDelay  пауза между попытками
 */
@ConfigurationProperties(prefix = "pulse")
public record NotificationProperties(
        String incidentsTopic,
        String deliveryUrl,
        Duration deliveryTimeout,
        int deliveryMaxAttempts,
        Duration deliveryRetryDelay) {
}
