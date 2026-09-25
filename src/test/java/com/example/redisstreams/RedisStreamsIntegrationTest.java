package com.example.redisstreams;

import com.example.redisstreams.service.HashService;
import com.example.redisstreams.service.RedisJsonService;
import com.example.redisstreams.service.StreamPublisher;
import com.example.redisstreams.service.StringValueService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(properties = {
        "app.redis.streams.names=events:test-one,events:test-two",
        "app.redis.streams.group=test-group",
        "app.redis.streams.consumer=test-consumer",
        "app.redis.streams.poll-timeout=100ms",
        "app.redis.streams.delete-after-ack=true"
})
class RedisStreamsIntegrationTest {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis/redis-stack-server:latest"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @Autowired HashService hashes;
    @Autowired RedisJsonService json;
    @Autowired StreamPublisher streams;
    @Autowired StringValueService strings;
    @Autowired StringRedisTemplate redis;
    @Autowired ObjectMapper mapper;

    @Test
    void stringCrudSupportsTtlAndAtomicCounters() {
        strings.set("config", "greeting", "hello", null);
        assertThat(strings.get("config", "greeting")).contains("hello");

        strings.set("counter", "visits", "10", Duration.ofMinutes(1));
        assertThat(strings.increment("counter", "visits", 5)).isEqualTo(15);
        assertThat(strings.delete("config", "greeting")).isTrue();
        assertThat(strings.get("config", "greeting")).isEmpty();
    }

    @Test
    void hashCrudUsesFieldOperations() {
        hashes.putAll("customer", "42", Map.of("name", "Ada", "tier", "gold"));
        assertThat(hashes.get("customer", "42", "name")).contains("Ada");
        assertThat(hashes.getFields("customer", "42", java.util.List.of("name", "tier")))
                .containsEntry("tier", "gold");
        assertThat(hashes.deleteField("customer", "42", "tier")).isTrue();
        assertThat(hashes.size("customer", "42")).isEqualTo(1);
        assertThat(hashes.delete("customer", "42")).isTrue();
    }

    @Test
    void jsonCrudSupportsPathsAndNumericIncrement() throws Exception {
        json.set("profile", "42", "$", mapper.readTree("{\"name\":\"Ada\",\"visits\":1}"));
        assertThat(json.get("profile", "42", "$.name")).isPresent();
        assertThat(json.increment("profile", "42", "$.visits", 2)).isEqualTo(3);
        assertThat(json.delete("profile", "42", "$.name")).isEqualTo(1);
        assertThat(json.get("profile", "42", "$.name")).isEmpty();
    }

    @Test
    void consumesAcknowledgesAndDeletesFromEveryConfiguredStream() throws Exception {
        streams.publish("events:test-one", Map.of("type", "created", "id", "1"));
        streams.publish("events:test-two", Map.of("type", "paid", "id", "2"));

        awaitStreamLength("events:test-one", 0);
        awaitStreamLength("events:test-two", 0);
    }

    private void awaitStreamLength(String stream, long expected) throws InterruptedException {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(10));
        while (Instant.now().isBefore(deadline)) {
            Long size = redis.opsForStream().size(stream);
            if (size != null && size == expected) {
                return;
            }
            Thread.sleep(100);
        }
        assertThat(redis.opsForStream().size(stream)).isEqualTo(expected);
    }
}
