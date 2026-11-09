package com.pulse.notification.config;

import com.pulse.notification.repository.NotificationRepository;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Возраст старейшей незавершённой доставки (P1-6): gauge
 * {@code pulse.notification.oldest.pending.age} — секунды с создания самой
 * старой строки {@code DELIVERING}, 0 если таких нет. После успешного старта
 * сервиса ненулевое значение означает строку, брошенную гибелью процесса
 * посреди доставки: повтор её не доставит (claim уже занят) — это сигнал
 * для человека.
 */
@Configuration
@EnableScheduling
public class NotificationDeliveryAgeMetrics {

    private final NotificationRepository repository;
    private final AtomicReference<Double> oldestPendingAge = new AtomicReference<>(0.0);

    public NotificationDeliveryAgeMetrics(NotificationRepository repository,
            MeterRegistry meterRegistry) {
        this.repository = repository;
        meterRegistry.gauge("pulse.notification.oldest.pending.age", oldestPendingAge,
                AtomicReference::get);
    }

    @Scheduled(fixedDelayString = "${pulse.pending-age-refresh:15s}")
    public void refresh() {
        oldestPendingAge.set(repository.oldestDeliveringAgeSeconds());
    }
}
