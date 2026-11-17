package com.pulse.metric.exception;

/** Kafka не подтвердила запись события за отведённое время (503). */
public class KafkaUnavailableException extends RuntimeException {

    public KafkaUnavailableException(Throwable cause) {
        super("Kafka не подтвердила запись события", cause);
    }
}
