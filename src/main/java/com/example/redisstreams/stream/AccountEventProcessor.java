package com.example.redisstreams.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.stereotype.Component;

@Component
public class AccountEventProcessor implements StreamSpecificProcessor {
    private static final String PREFIX = "t24_account_events:";
    private static final Logger log = LoggerFactory.getLogger(AccountEventProcessor.class);

    @Override
    public boolean supports(String stream) {
        return stream.startsWith(PREFIX);
    }

    @Override
    public void process(MapRecord<String, String, String> message) {
        String accountId = message.getStream().substring(PREFIX.length());
        // Replace this log statement with account-event business logic.
        log.info("Processing account event: accountId={} id={} body={}",
                accountId, message.getId(), message.getValue());
    }
}
