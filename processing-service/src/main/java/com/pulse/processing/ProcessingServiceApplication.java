package com.pulse.processing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class ProcessingServiceApplication {
    public static void main(String[] argv) {
        SpringApplication.run(ProcessingServiceApplication.class, argv);
    }
}
