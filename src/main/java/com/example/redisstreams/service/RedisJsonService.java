package com.example.redisstreams.service;

import com.example.redisstreams.domain.KeyNames;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

@Service
public class RedisJsonService {
    private static final RedisScript<String> JSON_INCREMENT = new DefaultRedisScript<>(
            "return tostring(redis.call('JSON.NUMINCRBY', KEYS[1], ARGV[1], ARGV[2]))", String.class);
    private static final RedisScript<String> JSON_DELETE = new DefaultRedisScript<>(
            "return tostring(redis.call('JSON.DEL', KEYS[1], ARGV[1]))", String.class);

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final KeyNames keys;

    public RedisJsonService(StringRedisTemplate redis, ObjectMapper objectMapper, KeyNames keys) {
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.keys = keys;
    }

    public void set(String namespace, String id, String path, JsonNode value) {
        execute("JSON.SET", keys.json(namespace, id), normalizePath(path), write(value));
    }

    public Optional<JsonNode> get(String namespace, String id, String path) {
        Object response = execute("JSON.GET", keys.json(namespace, id), normalizePath(path));
        if (response == null) {
            return Optional.empty();
        }
        try {
            JsonNode result = objectMapper.readTree(asString(response));
            return result.isArray() && result.isEmpty() ? Optional.empty() : Optional.of(result);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Redis returned invalid JSON", e);
        }
    }

    public long delete(String namespace, String id, String path) {
        String response = redis.execute(JSON_DELETE, List.of(keys.json(namespace, id)), normalizePath(path));
        return response == null ? 0 : Long.parseLong(response);
    }

    public double increment(String namespace, String id, String path, double amount) {
        String response = redis.execute(JSON_INCREMENT, List.of(keys.json(namespace, id)),
                normalizePath(path), Double.toString(amount));
        if (response == null) {
            throw new IllegalStateException("Redis did not return the incremented JSON value");
        }
        try {
            JsonNode result = objectMapper.readTree(response);
            JsonNode number = result.isArray() ? result.path(0) : result;
            if (!number.isNumber()) {
                throw new IllegalStateException("Redis returned a non-numeric JSON increment result: " + response);
            }
            return number.doubleValue();
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Redis returned an invalid JSON increment result", e);
        }
    }

    private Object execute(String command, String... arguments) {
        return redis.execute((RedisCallback<Object>) connection -> execute(connection, command, arguments));
    }

    private Object execute(RedisConnection connection, String command, String[] arguments) throws DataAccessException {
        byte[][] bytes = new byte[arguments.length][];
        for (int i = 0; i < arguments.length; i++) {
            bytes[i] = arguments[i].getBytes(StandardCharsets.UTF_8);
        }
        return connection.execute(command, bytes);
    }

    private String write(JsonNode value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid JSON", e);
        }
    }

    private String normalizePath(String path) {
        return path == null || path.isBlank() ? "$" : path;
    }

    private String asString(Object response) {
        if (response instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return response.toString();
    }
}
