package com.pulse.metric.exception;

/**
 * Зависимость чтения (ClickHouse/PostgreSQL) недоступна — circuit breaker разомкнут
 * или запрос не может быть выполнен. Клиенту отдаётся 503 (RFC 7807).
 */
public class DataUnavailableException extends RuntimeException {

    public DataUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
