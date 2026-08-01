package com.eventfanout;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class FanoutApplication {
    public static void main(String[] args) {
        SpringApplication.run(FanoutApplication.class, args);
    }
}
