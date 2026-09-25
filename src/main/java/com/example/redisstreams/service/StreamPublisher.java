package com.example.redisstreams.service;

import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class StreamPublisher {
    private final StringRedisTemplate redis;

    public StreamPublisher(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public RecordId publish(String stream, Map<String, String> body) {
        if (body.isEmpty()) {
            throw new IllegalArgumentException("A stream message must contain at least one field");
        }
        RecordId id = redis.opsForStream().add(StreamRecords.string(body).withStreamKey(stream));
        if (id == null) {
            throw new IllegalStateException("Redis did not return a stream record id");
        }
        return id;
    }
}
