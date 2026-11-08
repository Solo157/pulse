package com.pulse.processing.config;

import com.pulse.processing.service.OutboxService;
import com.pulse.processing.state.OutboxRepository;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.TimeUnit;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Фоновый оповещатель transactional outbox (раздел 8): по расписанию добирает
 * непроявленные события {@code IncidentDetected} — после сбоя Kafka и при старте
 * сервиса после падения с непроявленной очередью.
 *
 * <p>Отключается свойством {@code pulse.outbox-scheduling-enabled=false}
 * (интеграционные тесты прогоняют порции вручную).</p>
 */
@Configuration
@EnableScheduling
public class OutboxSchedulingConfig {

    @Component
    @ConditionalOnProperty(name = "pulse.outbox-scheduling-enabled", havingValue = "true",
            matchIfMissing = true)
    static class OutboxDrainer {

        private final OutboxService outboxService;
        private final ProcessingProperties properties;

        OutboxDrainer(OutboxService outboxService, ProcessingProperties properties) {
            this.outboxService = outboxService;
            this.properties = properties;
        }

        @Scheduled(fixedDelayString = "${pulse.outbox-poll-interval:1s}")
        public void drain() {
            outboxService.publishPending(properties.outboxBatchSize(),
                    properties.publishAckTimeout().multipliedBy(2),
                    properties.outboxPollInterval(), 5);
        }

        @Scheduled(fixedDelayString = "${pulse.outbox-poll-interval:1s}", initialDelayString = "10m")
        public void purge() {
            outboxService.purgePublished(properties.outboxRetention());
        }
    }

    /** Метрики очереди: глубина и возраст самой старой непроявленной записи. */
    @Component
    static class OutboxMetrics {

        OutboxMetrics(OutboxRepository repository, MeterRegistry meterRegistry) {
            meterRegistry.gauge("pulse.processing.outbox.pending", repository, r -> r.pendingCount());
            meterRegistry.gauge("pulse.processing.outbox.oldest-pending-seconds", repository,
                    r -> TimeUnit.MILLISECONDS.toSeconds(r.oldestPendingAge().toMillis()));
        }
    }
}
