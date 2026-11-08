package com.pulse.metric.exception;

/**
 * Конфликт повторного запроса (409): тот же Idempotency-Key, но другое тело,
 * либо гонка с политикой хранения (повторите запрос позже).
 */
public class IdempotencyConflictException extends RuntimeException {

    public IdempotencyConflictException() {
        super("Предыдущий запрос с этим Idempotency-Key ещё не завершён, повторите позже");
    }

    public IdempotencyConflictException(String message) {
        super(message);
    }
}
