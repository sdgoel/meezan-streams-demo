package com.example.redisstreams.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;

public class LoggingStreamMessageProcessor implements StreamMessageProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggingStreamMessageProcessor.class);

    @Override
    public void process(MapRecord<String, String, String> message) {
        log.info("Processed stream={} id={} body={}", message.getStream(), message.getId(), message.getValue());
    }
}
