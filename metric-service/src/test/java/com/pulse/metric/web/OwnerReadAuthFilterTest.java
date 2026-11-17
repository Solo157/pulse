package com.pulse.metric.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.description;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulse.metric.service.UserService;
import jakarta.servlet.FilterChain;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

/**
 * Логика контроля доступа read API (фильтр изолирован: только Mock-запрос/ответ
 * и замоканный {@link UserService}, без Spring-контекста и сети).
 */
@ExtendWith(MockitoExtension.class)
class OwnerReadAuthFilterTest {

    private static final UUID OWNER = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID STRANGER = UUID.fromString("22222222-2222-2222-2222-222222222222");

    @Mock
    private UserService userService;

    @Mock
    private FilterChain filterChain;

    @Test
    void shouldReturn401WithChallenge_whenAuthorizationHeaderAbsent() throws Exception {
        // Arrange
        OwnerReadAuthFilter filter = filter();
        MockHttpServletRequest request = get(OWNER);
        MockHttpServletResponse response = new MockHttpServletResponse();

        // Act
        filter.doFilter(request, response, filterChain);

        // Assert
        assertThat(response.getStatus()).as("401 без заголовка Authorization").isEqualTo(401);
        assertThat(response.getHeader("WWW-Authenticate"))
                .as("WWW-Authenticate должен предлагать Basic").contains("Basic");
        verify(filterChain, never().description("цепочка не продолжается без аутентификации"))
                .doFilter(any(), any());
    }

    @Test
    void shouldReturn401_whenPasswordWrong() throws Exception {
        // Arrange
        when(userService.passwordMatches(eq(OWNER), any())).thenReturn(false);
        MockHttpServletResponse response = new MockHttpServletResponse();

        // Act & Assert
        filter().doFilter(get(OWNER, OWNER), response, filterChain);
        assertThat(response.getStatus()).as("401 при неверном пароле").isEqualTo(401);
    }

    @Test
    void shouldReturn403_whenAuthenticatedUserIsNotPathOwner() throws Exception {
        // Arrange
        when(userService.passwordMatches(eq(STRANGER), any())).thenReturn(true);
        MockHttpServletResponse response = new MockHttpServletResponse();

        // Act & Assert
        filter().doFilter(pathOwner(OWNER, basic(STRANGER, "any-password")), response, filterChain);
        assertThat(response.getStatus()).as("403 для чужого userId в пути").isEqualTo(403);
    }

    @Test
    void shouldReturn401_whenLoginNotUuid() throws Exception {
        // Arrange
        MockHttpServletRequest request = get(OWNER, OWNER);
        request.removeHeader("Authorization");
        request.addHeader("Authorization", "Basic " + Base64.getEncoder().encodeToString(
                "admin:password".getBytes(StandardCharsets.UTF_8)));
        MockHttpServletResponse response = new MockHttpServletResponse();

        // Act & Assert
        filter().doFilter(request, response, filterChain);
        assertThat(response.getStatus()).as("401 для не-UUID логина").isEqualTo(401);
    }

    @Test
    void shouldProceed_whenOwnerReadsOwnPath() throws Exception {
        // Arrange
        when(userService.passwordMatches(eq(OWNER), any())).thenReturn(true);
        MockHttpServletRequest request = get(OWNER, OWNER);

        // Act
        MockHttpServletResponse response = new MockHttpServletResponse();
        filter().doFilter(request, response, filterChain);

        // Assert
        verify(filterChain, description("цепочка продолжается для владельца"))
                .doFilter(eq(request), eq(response));
    }

    @Test
    void shouldNotAuthenticatePostRequests() throws Exception {
        // Arrange
        MockHttpServletRequest request = get(OWNER, OWNER);
        request.setMethod("POST");
        MockHttpServletResponse response = new MockHttpServletResponse();

        // Act
        filter().doFilter(request, response, filterChain);

        // Assert
        verify(filterChain, description("POST ingest остаётся публичным"))
                .doFilter(request, response);
    }

    /** ObjectMapper без Spring: сериализует тот же ProblemDetail, что и контроллеры. */
    private OwnerReadAuthFilter filter() {
        return new OwnerReadAuthFilter(userService, new ObjectMapper());
    }

    private MockHttpServletRequest get(UUID pathOwner) {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setMethod("GET");
        request.setRequestURI("/users/" + pathOwner + "/metrics");
        return request;
    }

    private MockHttpServletRequest get(UUID pathOwner, UUID login) {
        MockHttpServletRequest request = get(pathOwner);
        request.addHeader("Authorization", basic(login, "any-password"));
        return request;
    }

    private MockHttpServletRequest pathOwner(UUID inPath, String headerValue) {
        MockHttpServletRequest request = get(inPath);
        request.addHeader("Authorization", headerValue);
        return request;
    }

    private String basic(UUID login, String password) {
        String raw = login + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }
}
