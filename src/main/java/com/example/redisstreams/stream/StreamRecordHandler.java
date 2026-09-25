package com.example.redisstreams.stream;

import com.example.redisstreams.config.StreamConsumerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

@Component
public class StreamRecordHandler implements StreamListener<String, MapRecord<String, String, String>> {
    private static final Logger log = LoggerFactory.getLogger(StreamRecordHandler.class);

    private final StringRedisTemplate redis;
    private final StreamConsumerProperties properties;
    private final StreamMessageProcessor processor;

    public StreamRecordHandler(StringRedisTemplate redis,
                               StreamConsumerProperties properties,
                               StreamMessageProcessor processor) {
        this.redis = redis;
        this.properties = properties;
        this.processor = processor;
    }

    @Override
    public void onMessage(MapRecord<String, String, String> message) {
        try {
            processor.process(message);
            Long acknowledged = redis.opsForStream()
                    .acknowledge(message.getStream(), properties.group(), message.getId());
            if (acknowledged == null || acknowledged != 1) {
                throw new IllegalStateException("Redis did not acknowledge stream message " + message.getId());
            }
            if (properties.deleteAfterAck()) {
                redis.opsForStream().delete(message.getStream(), message.getId());
            }
        } catch (RuntimeException exception) {
            log.error("Processing failed; stream message remains pending: stream={} id={}",
                    message.getStream(), message.getId(), exception);
        }
    }
}
