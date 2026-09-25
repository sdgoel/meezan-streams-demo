package com.example.redisstreams.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.stereotype.Component;

@Component
public class CurrencyEventProcessor implements StreamSpecificProcessor {
    private static final String PREFIX = "t24_currency_events:";
    private static final Logger log = LoggerFactory.getLogger(CurrencyEventProcessor.class);

    @Override
    public boolean supports(String stream) {
        return stream.startsWith(PREFIX);
    }

    @Override
    public void process(MapRecord<String, String, String> message) {
        String currency = message.getStream().substring(PREFIX.length());
        // Replace this log statement with currency-event business logic.
        log.info("Processing currency event: currency={} id={} body={}",
                currency, message.getId(), message.getValue());
    }
}
