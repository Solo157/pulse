package com.pulse.processing.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pulse.processing.config.ProcessingProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

class IncidentPublisherTest {

    private static final Instant FIXED_NOW = Instant.parse("2026-11-09T12:00:00Z");
    private static final String USER_ID = "6625e67b-6a87-4d02-91c5-b6eef4d45ae6";

    private KafkaTemplate<String, String> kafkaTemplate;
    private SimpleMeterRegistry meterRegistry;
    private IncidentPublisher publisher;

    @BeforeEach
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        meterRegistry = new SimpleMeterRegistry();
        ProcessingProperties properties = new ProcessingProperties(
                "metrics", "incidents",
                Duration.ofSeconds(60), Duration.ofSeconds(30), Duration.ofSeconds(5));
        publisher = new IncidentPublisher(kafkaTemplate,
                new ObjectMapper().registerModule(new JavaTimeModule()), properties,
                meterRegistry, Clock.fixed(FIXED_NOW, ZoneOffset.UTC));
    }

    @Test
    void shouldRecordLatencyFromSampleTimeToAck_whenOpenIncidentPublished() {
        // Arrange
        Instant sampleTime = FIXED_NOW.minusMillis(500);
        when(kafkaTemplate.send(eq("incidents"), eq(USER_ID), any(String.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        // Act
        publisher.publish(USER_ID, "rule-1", false, "temperature", 80.0, 85.0,
                sampleTime, "req-1");

        // Assert
        assertEquals(1L, meterRegistry.timer("pulse.e2e.incident.published").count(),
                "гистограмма должна получить одно наблюдение");
        assertEquals(0.5,
                meterRegistry.timer("pulse.e2e.incident.published").totalTime(java.util.concurrent.TimeUnit.SECONDS),
                "наблюдение = время от sampleTime до подтверждения брокера");
    }

    @Test
    void shouldNotRecordLatency_whenCloseIncidentPublished() {
        // Arrange
        when(kafkaTemplate.send(eq("incidents"), eq(USER_ID), any(String.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        // Act
        publisher.publish(USER_ID, "rule-1", true, "temperature", 80.0, 20.0,
                FIXED_NOW.minusSeconds(30), "req-1");

        // Assert
        assertEquals(0L, meterRegistry.timer("pulse.e2e.incident.published").count(),
                "закрытие инцидента не участвует в задержке обнаружения");
    }

    @Test
    void shouldThrowAndNotRecordLatency_whenBrokerNotAcknowledged() {
        // Arrange
        CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("kafka down"));
        when(kafkaTemplate.send(eq("incidents"), eq(USER_ID), any(String.class)))
                .thenReturn(failed);

        // Act & Assert
        assertThrows(IncidentPublisher.IncidentPublishException.class,
                () -> publisher.publish(USER_ID, "rule-1", false, "temperature", 80.0, 85.0,
                        FIXED_NOW.minusSeconds(1), "req-1"),
                "подтверждение брокера не получено — публикация считается неудачной");
        assertEquals(0L, meterRegistry.timer("pulse.e2e.incident.published").count(),
                "неудачная публикация не добавляет наблюдений в гистограмму");
        verify(kafkaTemplate).send(eq("incidents"), eq(USER_ID), any(String.class));
    }
}
