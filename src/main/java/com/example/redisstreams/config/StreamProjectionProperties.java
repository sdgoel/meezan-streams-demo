package com.example.redisstreams.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;
import java.util.regex.Pattern;

@ConfigurationProperties("app.redis.stream-projections")
public record StreamProjectionProperties(List<Definition> definitions) {
    private static final Pattern STREAM = Pattern.compile("[a-zA-Z0-9:._-]{1,200}");
    private static final Pattern PREFIX = Pattern.compile("[a-zA-Z0-9:_-]{1,100}");
    private static final Pattern C_VALUE = Pattern.compile("c[0-9]+");

    public StreamProjectionProperties {
        definitions = definitions == null ? List.of() : List.copyOf(definitions);
        long uniqueStreams = definitions.stream().map(Definition::stream).distinct().count();
        if (uniqueStreams != definitions.size()) {
            throw new IllegalArgumentException("Each XML projection must have a unique stream name");
        }
    }

    public record Definition(String stream, String keyPrefix, List<String> cValues) {
        public Definition {
            if (stream == null || !STREAM.matcher(stream).matches()) {
                throw new IllegalArgumentException("Invalid projection stream name: " + stream);
            }
            if (keyPrefix == null || !PREFIX.matcher(keyPrefix).matches()) {
                throw new IllegalArgumentException("Invalid projection key prefix: " + keyPrefix);
            }
            cValues = cValues == null ? List.of() : cValues.stream()
                    .map(String::trim)
                    .map(String::toLowerCase)
                    .distinct()
                    .toList();
            if (cValues.isEmpty() || cValues.stream().anyMatch(value -> !C_VALUE.matcher(value).matches())) {
                throw new IllegalArgumentException("Projection c-values must look like c176 or c178");
            }
        }
    }
}
