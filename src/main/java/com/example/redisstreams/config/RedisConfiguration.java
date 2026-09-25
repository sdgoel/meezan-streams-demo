package com.example.redisstreams.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;

@Configuration
public class RedisConfiguration {

    @Bean(destroyMethod = "stop")
    StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamListenerContainer(
            RedisConnectionFactory connectionFactory,
            StreamConsumerProperties properties) {
        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String,
                MapRecord<String, String, String>> options = StreamMessageListenerContainer
                .StreamMessageListenerContainerOptions.builder()
                .pollTimeout(properties.pollTimeout())
                .build();
        return StreamMessageListenerContainer.create(connectionFactory, options);
    }
}
