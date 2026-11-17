package com.pulse.metric.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import org.springframework.core.annotation.Order;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Отсекает запросы больше лимита тела ещё до чтения JSON (413, согласование п. 5).
 * Ответ формируется в том же формате RFC 7807.
 */
@Component
@Order(2)
public class BodySizeLimitFilter extends OncePerRequestFilter {

    private final long maxBodyBytes;
    private final ObjectMapper objectMapper;

    public BodySizeLimitFilter(PulseProperties properties, ObjectMapper objectMapper) {
        this.maxBodyBytes = properties.maxRequestBodyBytes();
        this.objectMapper = objectMapper;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        long contentLength = request.getContentLengthLong();
        if (contentLength > maxBodyBytes) {
            response.setStatus(HttpStatus.PAYLOAD_TOO_LARGE.value());
            response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
            ProblemDetail problemDetail = ProblemDetail.forStatusAndDetail(
                    HttpStatus.PAYLOAD_TOO_LARGE,
                    "Тело запроса больше лимита " + maxBodyBytes + " байт");
            objectMapper.writeValue(response.getOutputStream(), problemDetail);
            return;
        }
        filterChain.doFilter(request, response);
    }
}
