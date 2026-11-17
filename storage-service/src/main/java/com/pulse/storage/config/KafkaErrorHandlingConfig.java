package com.pulse.storage.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Стратегия повторной доставки (согласование п. 9 и раздел 6.3): при ошибке вставки
 * offset НЕ двигается, пачка повторяется с backoff. ClickHouse лежит — группа ждёт,
 * данные не теряются и не коммитятся вхолостую.
 */
@Configuration
public class KafkaErrorHandlingConfig {

    @Bean
    public CommonErrorHandler kafkaErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(2000L, FixedBackOff.UNLIMITED_ATTEMPTS));
    }
}
