package com.pulse.metric.config;

import com.zaxxer.hikari.HikariDataSource;
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
    // (она @ConditionalOnMissingBean), поэтому PostgreSQL-источник объявлен явно и с @Primary.
    // Таймауты пула связываются @ConfigurationProperties напрямую с HikariDataSource —
    // без этого spring.*.hikari.* в yml осталась бы мёртвой строкой.
    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource")
    public DataSourceProperties postgresDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource.hikari")
    public DataSource dataSource() {
        return postgresDataSourceProperties().initializeDataSourceBuilder()
                .type(HikariDataSource.class).build();
    }

    /** Свойства второго (ClickHouse) источника из {@code spring.clickhouse.*}. */
    @Bean
    @ConfigurationProperties("spring.clickhouse")
    public DataSourceProperties clickhouseDataSourceProperties() {
        return new DataSourceProperties();
    }

    /** DataSource истории метрик (ClickHouse), таймауты пула — из spring.clickhouse.hikari.*. */
    @Bean
    @ConfigurationProperties("spring.clickhouse.hikari")
    public DataSource clickhouseDataSource() {
        return clickhouseDataSourceProperties().initializeDataSourceBuilder()
                .type(HikariDataSource.class).build();
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
