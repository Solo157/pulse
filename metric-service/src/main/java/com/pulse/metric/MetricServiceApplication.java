package com.pulse.metric;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class MetricServiceApplication {
    public static void main(String[] argv) {
        SpringApplication.run(MetricServiceApplication.class, argv);
    }
}
