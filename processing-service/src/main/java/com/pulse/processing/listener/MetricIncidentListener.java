package com.pulse.processing.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulse.contracts.event.MetricEvent;
import com.pulse.processing.service.IncidentEvaluator;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Группа {@code processing-group}: читает {@code metrics}, оценивает правила
 * и публикует {@code IncidentDetected} (раздел 6.4).
 *
 * <p>Обработка at-least-once: повтор пакета повторит переходы state-машины,
 * которые на повторе не создают второго инцидента (переход open уже совершён).</p>
 *
 * <p>Гистограмма {@code pulse.event.age} — возраст пакета на входе потребителя
 * от acceptedAt (FR «обнаружение инцидента до 10/20 с» измеряется по её квантилям).</p>
 */
@Component
public class MetricIncidentListener {

    private static final Logger log = LoggerFactory.getLogger(MetricIncidentListener.class);

    private final IncidentEvaluator evaluator;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final Timer eventAgeTimer;

    public MetricIncidentListener(IncidentEvaluator evaluator, ObjectMapper objectMapper,
            MeterRegistry meterRegistry, Clock clock) {
        this.evaluator = evaluator;
        this.objectMapper = objectMapper;
        this.clock = clock;
        this.eventAgeTimer = meterRegistry.timer("pulse.event.age", "consumer", "processing-metrics");
    }

    @KafkaListener(id = "processing-metrics", topics = "${pulse.metrics-topic}")
    public void onMetricEvent(String payload) {
        MetricEvent event = parse(payload);
        Instant acceptedAt = event.acceptedAt();
        if (acceptedAt != null) {
            eventAgeTimer.record(Duration.between(acceptedAt, Instant.now(clock)));
        }
        log.debug("Обработка пакета {} точек пользователя {}", event.points().size(), event.userId());
        evaluator.process(event);
    }

    private MetricEvent parse(String payload) {
        try {
            return objectMapper.readValue(payload, MetricEvent.class);
        } catch (Exception failure) {
            throw new IllegalArgumentException("Не разобрано событие MetricEvent: " + payload, failure);
        }
    }
}
