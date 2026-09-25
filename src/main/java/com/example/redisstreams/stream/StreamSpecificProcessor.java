package com.example.redisstreams.stream;

import org.springframework.data.redis.connection.stream.MapRecord;

/**
 * Business logic for one stream key or one family of stream keys.
 */
public interface StreamSpecificProcessor {
    boolean supports(String stream);

    void process(MapRecord<String, String, String> message);
}
