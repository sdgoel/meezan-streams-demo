package com.example.redisstreams.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.stereotype.Component;

@Component
public class CustomerRawEventProcessor implements StreamSpecificProcessor {
    private static final String PREFIX = "t24_customer_raw_events:";
    private static final Logger log = LoggerFactory.getLogger(CustomerRawEventProcessor.class);

    @Override
    public boolean supports(String stream) {
        return stream.startsWith(PREFIX);
    }

    @Override
    public void process(MapRecord<String, String, String> message) {
        String rowId = message.getStream().substring(PREFIX.length());
        String document = message.getValue().get("doc");
        String operationCode = message.getValue().get("op_code");

        // Replace this log statement with customer-row business logic.
        log.info("Processing customer row: rowId={} operation={} id={} document={}",
                rowId, operationCode, message.getId(), document);
    }
}
