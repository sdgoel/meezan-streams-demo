package com.example.redisstreams;

import com.example.redisstreams.config.StreamConsumerProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(StreamConsumerProperties.class)
public class RedisStreamsApplication {

    public static void main(String[] args) {
        SpringApplication.run(RedisStreamsApplication.class, args);
    }
}
