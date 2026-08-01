package com.eventfanout.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

@Configuration
public class StorageConfig {

    @Bean
    Path batchesDir(@Value("${fanout.batches-dir:data/batches}") String batchesDir) {
        return Path.of(batchesDir);
    }
}
