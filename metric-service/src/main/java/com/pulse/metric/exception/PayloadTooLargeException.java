package com.pulse.metric.exception;

/** Превышен лимит размера запроса (413). */
public class PayloadTooLargeException extends RuntimeException {

    public PayloadTooLargeException(String detail) {
        super(detail);
    }
}
