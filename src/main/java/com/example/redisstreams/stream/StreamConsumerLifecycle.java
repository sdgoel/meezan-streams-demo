package com.example.redisstreams.stream;

import com.example.redisstreams.config.StreamConsumerProperties;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class StreamConsumerLifecycle {
    private static final Logger log = LoggerFactory.getLogger(StreamConsumerLifecycle.class);

    private final StringRedisTemplate redis;
    private final StreamMessageListenerContainer<String, MapRecord<String, String, String>> container;
    private final StreamRecordHandler handler;
    private final StreamConsumerProperties properties;

    public StreamConsumerLifecycle(StringRedisTemplate redis,
                                   StreamMessageListenerContainer<String, MapRecord<String, String, String>> container,
                                   StreamRecordHandler handler,
                                   StreamConsumerProperties properties) {
        this.redis = redis;
        this.container = container;
        this.handler = handler;
        this.properties = properties;
    }

    @PostConstruct
    void start() {
        for (String stream : properties.names()) {
            createGroup(stream);
            var request = StreamMessageListenerContainer.StreamReadRequest
                    .builder(StreamOffset.create(stream, ReadOffset.lastConsumed()))
                    .consumer(Consumer.from(properties.group(), properties.consumer()))
                    .autoAcknowledge(false)
                    .cancelOnError(error -> false)
                    .errorHandler(error -> log.error("Redis stream read failed for stream={}; retrying", stream, error))
                    .build();
            container.register(request, handler);
            log.info("Listening to Redis stream={} group={} consumer={}",
                    stream, properties.group(), properties.consumer());
        }
        container.start();
    }

    private void createGroup(String stream) {
        try {
            redis.execute((RedisCallback<Object>) connection -> connection.execute(
                    "XGROUP",
                    bytes("CREATE"), bytes(stream), bytes(properties.group()), bytes("0-0"), bytes("MKSTREAM")));
        } catch (RuntimeException exception) {
            if (!containsBusyGroup(exception)) {
                throw exception;
            }
        }
    }

    private boolean containsBusyGroup(Throwable throwable) {
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            if (current.getMessage() != null && current.getMessage().contains("BUSYGROUP")) {
                return true;
            }
        }
        return false;
    }

    private byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
