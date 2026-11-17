package com.pulse.metric.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulse.metric.web.OwnerReadAuthFilter;
import com.pulse.metric.service.UserService;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

/**
 * Криптосторона пользователей (Этап 5): BCrypt для хранения паролей.
 * Полноценный Spring Security не подключается — авторизацию read-эндпоинтов
 * делает лёгкий сервлет-фильтр {@link OwnerReadAuthFilter}.
 */
@Configuration
public class SecurityConfig {

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    /**
     * Basic-аутентификация владельца на read-эндпоинтах /users/* (GET).
     * Отдельный фильтр, а не Spring Security: в ДЕМО нужна ровно одна проверка
     * «запросил свои данные», без фильтров сессий/CSRF/редиректов.
     */
    @Bean
    public FilterRegistrationBean<OwnerReadAuthFilter> ownerReadAuthFilter(
            UserService userService, ObjectMapper objectMapper) {
        FilterRegistrationBean<OwnerReadAuthFilter> registration =
                new FilterRegistrationBean<>(new OwnerReadAuthFilter(userService, objectMapper));
        registration.addUrlPatterns("/users/*");
        registration.setOrder(100);
        return registration;
    }
}
