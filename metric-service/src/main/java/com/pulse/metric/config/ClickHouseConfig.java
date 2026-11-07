package com.pulse.metric.config;

import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Read-доступ к истории метрик (Этап 5): второй источник данных — ClickHouse.
 * PostgreSQL остаётся основным ({@code @Primary}) источником MetricService.
 */
@Configuration
public class ClickHouseConfig {

    // Собственные бины DataSource выключают автоконфигурацию DataSourceAutoConfiguration
    // (она @ConditionalOnMissingBean), поэтому PostgreSQL-источник объявлен явно иPrimary.
    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource")
    public DataSourceProperties postgresDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean
    @Primary
    public DataSource dataSource() {
        return postgresDataSourceProperties().initializeDataSourceBuilder().build();
    }

    /** Свойства второго (ClickHouse) источника из {@code spring.clickhouse.*}. */
    @Bean
    @ConfigurationProperties("spring.clickhouse")
    public DataSourceProperties clickhouseDataSourceProperties() {
        return new DataSourceProperties();
    }

    /** DataSource истории метрик (ClickHouse). */
    @Bean
    public DataSource clickhouseDataSource() {
        return clickhouseDataSourceProperties().initializeDataSourceBuilder().build();
    }

    /** JdbcTemplate для чтения истории метрик. */
    @Bean
    public JdbcTemplate clickhouseJdbcTemplate() {
        return new JdbcTemplate(clickhouseDataSource());
    }

    @Bean
    @Primary
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
