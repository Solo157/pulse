package com.pulse.storage.config;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Часы приложения: системные, но инжектируемые — сквозные задержки
 * измеряются детерминированно (в тестах подменяются фиксированными).
 */
@Configuration
public class ClockConfig {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
