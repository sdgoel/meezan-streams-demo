package com.example.redisstreams.stream;

import org.springframework.data.redis.connection.stream.MapRecord;

@FunctionalInterface
public interface StreamMessageProcessor {
    void process(MapRecord<String, String, String> message);
}
