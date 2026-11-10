package com.pulse.notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class NotificationServiceApplication {
    public static void main(String[] argv) {
        SpringApplication.run(NotificationServiceApplication.class, argv);
    }
}
