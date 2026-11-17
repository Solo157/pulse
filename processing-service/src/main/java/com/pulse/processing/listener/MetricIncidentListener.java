package com.pulse.processing.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulse.contracts.event.MetricEvent;
import com.pulse.processing.service.IncidentEvaluator;
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
 * <p>FR «обнаружение инцидента до 10/20 с» измеряется гистограммой
 * {@code pulse.e2e.incident.published} в {@code IncidentPublisher}.</p>
 */
@Component
public class MetricIncidentListener {

    private static final Logger log = LoggerFactory.getLogger(MetricIncidentListener.class);

    private final IncidentEvaluator evaluator;
    private final ObjectMapper objectMapper;

    public MetricIncidentListener(IncidentEvaluator evaluator, ObjectMapper objectMapper) {
        this.evaluator = evaluator;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(id = "processing-metrics", topics = "${pulse.metrics-topic}")
    public void onMetricEvent(String payload) {
        MetricEvent event = parse(payload);
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
