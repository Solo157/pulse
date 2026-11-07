package com.pulse.storage.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulse.contracts.event.MetricEvent;
import com.pulse.storage.service.ClickHousePointsWriter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Группа storage-group читает топик metrics и пишет точки в ClickHouse (FR, раздел 6.3).
 * Коммит offset'ов выполняется батче-контейнером только после успешной вставки
 * всей пачки (spring.listener.ack-mode=batch); при ошибке вставки offset не двигается,
 * контейнер повторяет пачку с backoff.
 */
@Component
public class MetricEventConsumerListener {

    private static final Logger log = LoggerFactory.getLogger(MetricEventConsumerListener.class);

    private final ClickHousePointsWriter writer;
    private final ObjectMapper objectMapper;
    private final Timer insertTimer;

    public MetricEventConsumerListener(ClickHousePointsWriter writer,
                                       ObjectMapper objectMapper,
                                       MeterRegistry meterRegistry) {
        this.writer = writer;
        this.objectMapper = objectMapper;
        this.insertTimer = meterRegistry.timer("pulse.storage.insert.duration");
    }

    /** Одна пачка (poll) обрабатывается одним вызовом: максимум max-poll-records событий. */
    @KafkaListener(topics = "${pulse.metrics-topic}", groupId = "storage-group")
    public void onBatch(List<ConsumerRecord<String, String>> records) {
        insertTimer.record(() -> {
            for (ConsumerRecord<String, String> record : records) {
                process(record);
            }
        });
    }

    private void process(ConsumerRecord<String, String> record) {
        MetricEvent event;
        try {
            event = objectMapper.readValue(record.value(), MetricEvent.class);
        } catch (Exception exception) {
            // Битое сообщение нельзя пропускать молча: оно блокирует пачку до вмешательства.
            log.error("Нечитаемое событие из топика {} partition {} offset {} — повтор не поможет",
                    record.topic(), record.partition(), record.offset(), exception);
            throw new IllegalStateException("Нечитаемое событие MetricEvent", exception);
        }
        MDC.put("requestId", event.requestId());
        try {
            writer.insertPoints(event.userId(), event.eventId(), event.points());
        } finally {
            MDC.remove("requestId");
        }
    }
}
