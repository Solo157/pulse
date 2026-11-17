package com.pulse.metric.exception;

/** Предыдущий запрос с тем же Idempotency-Key ещё не завершён (409). */
public class IdempotencyConflictException extends RuntimeException {

    public IdempotencyConflictException() {
        super("Предыдущий запрос с этим Idempotency-Key ещё не завершён, повторите позже");
    }
}
