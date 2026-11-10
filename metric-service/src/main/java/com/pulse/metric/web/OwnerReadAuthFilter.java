package com.pulse.metric.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulse.metric.config.RequestIdFilter;
import com.pulse.metric.service.UserService;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Контроль доступа владельца к read API (FR-4, Этап 5): GET /users/{userId}/...
 * требует Basic-аутентификацию (login = userId, password = пароль регистрации),
 * причём аутентифицированный пользователь должен совпадать с {userId} в пути.
 * Не владелица/чужой userId → 403, отсутствие/неверный кред → 401.
 * Ответы — тот же RFC 7807, что и у контроллеров.
 */
public class OwnerReadAuthFilter extends OncePerRequestFilter {

    private static final String BASIC_PREFIX = "Basic ";

    private final UserService userService;
    private final ObjectMapper objectMapper;

    public OwnerReadAuthFilter(UserService userService, ObjectMapper objectMapper) {
        this.userService = userService;
        this.objectMapper = objectMapper;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !"GET".equalsIgnoreCase(request.getMethod());
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        String header = request.getHeader("Authorization");
        if (header == null || !header.startsWith(BASIC_PREFIX)) {
            reject(response, HttpStatus.UNAUTHORIZED, "Требуется Basic-аутентификация");
            return;
        }

        String login;
        String password;
        try {
            String decoded = new String(Base64.getDecoder().decode(
                    header.substring(BASIC_PREFIX.length()).trim()), StandardCharsets.UTF_8);
            int separator = decoded.indexOf(':');
            if (separator < 0) {
                reject(response, HttpStatus.UNAUTHORIZED, "Некорректный формат Basic-заголовка");
                return;
            }
            login = decoded.substring(0, separator);
            password = decoded.substring(separator + 1);
        } catch (IllegalArgumentException malformed) {
            reject(response, HttpStatus.UNAUTHORIZED, "Некорректный формат Basic-заголовка");
            return;
        }

        UUID authenticated;
        try {
            authenticated = UUID.fromString(login);
        } catch (IllegalArgumentException notUuid) {
            reject(response, HttpStatus.UNAUTHORIZED, "Логин должен быть идентификатором пользователя");
            return;
        }

        if (!userService.passwordMatches(authenticated, password)) {
            reject(response, HttpStatus.UNAUTHORIZED, "Неверные учётные данные");
            return;
        }

        UUID pathOwner = pathOwnerOrNull(request);
        if (pathOwner == null) {
            reject(response, HttpStatus.UNAUTHORIZED, "Некорректный путь запроса");
            return;
        }
        if (!pathOwner.equals(authenticated)) {
            reject(response, HttpStatus.FORBIDDEN, "Доступ к данным другого пользователя запрещён");
            return;
        }

        filterChain.doFilter(request, response);
    }

    private UUID pathOwnerOrNull(HttpServletRequest request) {
        // Путь зарегистрирован как /users/*: сегменты ["", "users", "{userId}", ...].
        String[] segments = request.getRequestURI().split("/");
        if (segments.length < 3) {
            return null;
        }
        try {
            return UUID.fromString(segments[2]);
        } catch (IllegalArgumentException notUuid) {
            return null;
        }
    }

    private void reject(HttpServletResponse response, HttpStatus status, String detail)
            throws IOException {
        response.setStatus(status.value());
        if (status == HttpStatus.UNAUTHORIZED) {
            response.setHeader("WWW-Authenticate", "Basic realm=\"pulse\", charset=\"UTF-8\"");
        }
        ProblemDetail problemDetail = ProblemDetail.forStatusAndDetail(status, detail);
        problemDetail.setProperty("requestId", MDC.get(RequestIdFilter.REQUEST_ID_MDC_KEY));
        response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
        objectMapper.writeValue(response.getOutputStream(), problemDetail);
    }
}
