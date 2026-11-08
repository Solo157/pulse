package com.pulse.metric.config;

import com.pulse.metric.repository.IngestOutboxRepository;
import com.pulse.metric.service.IngestOutboxService;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.TimeUnit;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Фоновый отправитель transactional outbox-ingest: по расписанию добирает
 * принятые, но не подтверждённые брокером MetricEvent — после сбоя Kafka и при
 * старте сервиса после падения с непроявленной очередью.
 *
 * <p>Отключается свойством {@code pulse.outbox-scheduling-enabled=false}
 * (интеграционные тесты прогоняют порции вручную).</p>
 */
@Configuration
@EnableScheduling
public class IngestOutboxSchedulingConfig {

    @Component
    @ConditionalOnProperty(name = "pulse.outbox-scheduling-enabled", havingValue = "true",
            matchIfMissing = true)
    static class IngestOutboxDrainer {

        private final IngestOutboxService outboxService;
        private final PulseProperties properties;

        IngestOutboxDrainer(IngestOutboxService outboxService, PulseProperties properties) {
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
            outboxService.purge(properties.outboxRetention(), properties.idempotencyRetention());
        }
    }

    /** Метрики очереди: глубина и возраст самой старой непроявленной записи. */
    @Component
    static class IngestOutboxMetrics {

        IngestOutboxMetrics(IngestOutboxRepository repository, MeterRegistry meterRegistry) {
            meterRegistry.gauge("pulse.ingest.outbox.pending", repository, r -> r.pendingCount());
            meterRegistry.gauge("pulse.ingest.outbox.oldest-pending-seconds", repository,
                    r -> TimeUnit.MILLISECONDS.toSeconds(r.oldestPendingAge().toMillis()));
        }
    }
}
