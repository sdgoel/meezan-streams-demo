package com.example.redisstreams.service;

import com.example.redisstreams.domain.KeyNames;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class HashService {
    private final StringRedisTemplate redis;
    private final KeyNames keys;

    public HashService(StringRedisTemplate redis, KeyNames keys) {
        this.redis = redis;
        this.keys = keys;
    }

    public void putAll(String namespace, String id, Map<String, String> fields) {
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("At least one hash field is required");
        }
        redis.opsForHash().putAll(keys.hash(namespace, id), fields);
    }

    public void put(String namespace, String id, String field, String value) {
        redis.opsForHash().put(keys.hash(namespace, id), field, value);
    }

    public Optional<String> get(String namespace, String id, String field) {
        Object value = redis.opsForHash().get(keys.hash(namespace, id), field);
        return Optional.ofNullable(value).map(Object::toString);
    }

    public Map<String, String> getFields(String namespace, String id, List<String> fields) {
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("At least one field must be requested");
        }
        List<Object> values = redis.opsForHash().multiGet(keys.hash(namespace, id), List.copyOf(fields));
        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Object value = values.get(i);
            if (value != null) {
                result.put(fields.get(i), value.toString());
            }
        }
        return result;
    }

    public boolean deleteField(String namespace, String id, String field) {
        return redis.opsForHash().delete(keys.hash(namespace, id), field) > 0;
    }

    public boolean delete(String namespace, String id) {
        return Boolean.TRUE.equals(redis.delete(keys.hash(namespace, id)));
    }

    public long size(String namespace, String id) {
        return redis.opsForHash().size(keys.hash(namespace, id));
    }
}
