package com.pulse.storage;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class StorageServiceApplication {
    public static void main(String[] argv) {
        SpringApplication.run(StorageServiceApplication.class, argv);
    }
}
