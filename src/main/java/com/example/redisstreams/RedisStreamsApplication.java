package com.example.redisstreams;

import com.example.redisstreams.config.StreamConsumerProperties;
import com.example.redisstreams.config.StreamProjectionProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties({StreamConsumerProperties.class, StreamProjectionProperties.class})
public class RedisStreamsApplication {

    public static void main(String[] args) {
        SpringApplication.run(RedisStreamsApplication.class, args);
    }
}
