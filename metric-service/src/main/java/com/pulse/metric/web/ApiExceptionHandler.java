package com.pulse.metric.web;

import com.pulse.metric.config.RequestIdFilter;
import com.pulse.metric.exception.EmailAlreadyRegisteredException;
import com.pulse.metric.exception.IdempotencyConflictException;
import com.pulse.metric.exception.KafkaUnavailableException;
import com.pulse.metric.exception.PayloadTooLargeException;
import com.pulse.metric.exception.UserNotFoundException;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

/**
 * Единый формат ошибок — RFC 7807 (application/problem+json) с correlation id
 * (согласование п. 6). Текст ошибки русскоязычный, secrets не попадаёт.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail invalidArgument(IllegalArgumentException exception) {
        return problem(HttpStatus.BAD_REQUEST, exception.getMessage());
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ProblemDetail malformedBody(HttpMessageNotReadableException exception) {
        return problem(HttpStatus.BAD_REQUEST, "Не читается JSON тела запроса");
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ProblemDetail wrongPathArgument(MethodArgumentTypeMismatchException exception) {
        return problem(HttpStatus.BAD_REQUEST, "Некорректный идентификатор пользователя");
    }

    @ExceptionHandler(MissingRequestHeaderException.class)
    public ProblemDetail missingHeader(MissingRequestHeaderException exception) {
        return problem(HttpStatus.BAD_REQUEST,
                "Обязательный заголовок отсутствует: " + exception.getHeaderName());
    }

    @ExceptionHandler(UserNotFoundException.class)
    public ProblemDetail userNotFound(UserNotFoundException exception) {
        return problem(HttpStatus.NOT_FOUND, exception.getMessage());
    }

    @ExceptionHandler({EmailAlreadyRegisteredException.class, IdempotencyConflictException.class})
    public ProblemDetail conflict(RuntimeException exception) {
        return problem(HttpStatus.CONFLICT, exception.getMessage());
    }

    @ExceptionHandler(PayloadTooLargeException.class)
    public ProblemDetail tooLarge(PayloadTooLargeException exception) {
        return problem(HttpStatus.PAYLOAD_TOO_LARGE, exception.getMessage());
    }

    @ExceptionHandler(KafkaUnavailableException.class)
    public ProblemDetail kafkaUnavailable(KafkaUnavailableException exception) {
        return problem(HttpStatus.SERVICE_UNAVAILABLE, "Сервис временно недоступен, попробуйте позже");
    }

    private ProblemDetail problem(HttpStatus status, String detail) {
        ProblemDetail problemDetail = ProblemDetail.forStatusAndDetail(status, detail);
        problemDetail.setProperty("requestId", MDC.get(RequestIdFilter.REQUEST_ID_MDC_KEY));
        return problemDetail;
    }
}
