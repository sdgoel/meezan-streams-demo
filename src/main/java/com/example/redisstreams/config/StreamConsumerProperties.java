package com.example.redisstreams.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.util.List;

@Validated
@ConfigurationProperties("app.redis.streams")
public record StreamConsumerProperties(
        @NotEmpty List<@NotBlank String> names,
        @NotBlank String group,
        @NotBlank String consumer,
        Duration pollTimeout,
        boolean deleteAfterAck
) {
    public StreamConsumerProperties {
        pollTimeout = pollTimeout == null ? Duration.ofSeconds(2) : pollTimeout;
        names = names == null ? List.of() : List.copyOf(names);
    }
}
