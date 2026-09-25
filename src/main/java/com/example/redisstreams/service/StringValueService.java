package com.example.redisstreams.service;

import com.example.redisstreams.domain.KeyNames;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;

@Service
public class StringValueService {
    private final StringRedisTemplate redis;
    private final KeyNames keys;

    public StringValueService(StringRedisTemplate redis, KeyNames keys) {
        this.redis = redis;
        this.keys = keys;
    }

    public void set(String namespace, String id, String value, Duration ttl) {
        String key = keys.string(namespace, id);
        if (ttl == null) {
            redis.opsForValue().set(key, value);
            return;
        }
        if (ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("TTL must be greater than zero");
        }
        redis.opsForValue().set(key, value, ttl);
    }

    public Optional<String> get(String namespace, String id) {
        return Optional.ofNullable(redis.opsForValue().get(keys.string(namespace, id)));
    }

    public long increment(String namespace, String id, long amount) {
        Long value = redis.opsForValue().increment(keys.string(namespace, id), amount);
        if (value == null) {
            throw new IllegalStateException("Redis did not return the incremented value");
        }
        return value;
    }

    public boolean delete(String namespace, String id) {
        return Boolean.TRUE.equals(redis.delete(keys.string(namespace, id)));
    }
}
