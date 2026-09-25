package com.example.redisstreams.config;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Code-level business configuration for each T24 stream.
 * Add or change selected c-values here when application defaults change.
 */
@Component
public class T24StreamProjectionCatalog {
    private static final Pattern STREAM = Pattern.compile("[a-zA-Z0-9:._-]{1,200}");
    private static final Pattern PREFIX = Pattern.compile("[a-zA-Z0-9:_-]{1,100}");
    private static final Pattern C_VALUE = Pattern.compile("c[0-9]+");

    private final Map<String, Definition> definitions = Map.of(
            "t24_customer_events", new Definition(
                    "t24_customer_events", "t24_customer", List.of("c176", "c178")),
            "t24_currency_events", new Definition(
                    "t24_currency_events", "t24_currency", List.of("c1", "c2")),
            "t24_cusrtomer_events", new Definition(
                    "t24_cusrtomer_events", "t24_cusrtomer", List.of("c176", "c178"))
    );

    public Optional<Definition> find(String stream) {
        return Optional.ofNullable(definitions.get(stream));
    }

    public record Definition(String stream, String keyPrefix, List<String> defaultCValues) {
        public Definition {
            if (stream == null || !STREAM.matcher(stream).matches()) {
                throw new IllegalArgumentException("Invalid projection stream name: " + stream);
            }
            if (keyPrefix == null || !PREFIX.matcher(keyPrefix).matches()) {
                throw new IllegalArgumentException("Invalid projection key prefix: " + keyPrefix);
            }
            defaultCValues = normalizeCValues(defaultCValues);
        }
    }

    public static List<String> normalizeCValues(List<String> values) {
        List<String> normalized = values == null ? List.of() : values.stream()
                .map(String::trim)
                .map(String::toLowerCase)
                .distinct()
                .toList();
        if (normalized.isEmpty() || normalized.size() > 100
                || normalized.stream().anyMatch(value -> !C_VALUE.matcher(value).matches())) {
            throw new IllegalArgumentException(
                    "One to 100 c-values are required; each must look like c176 or c178");
        }
        return normalized;
    }
}
