package com.example.redisstreams.stream;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.StreamRecords;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StreamProcessorRouterTest {

    @Test
    void dispatchesToTheProcessorForTheConcreteStream() {
        AtomicInteger accountCalls = new AtomicInteger();
        AtomicInteger customerCalls = new AtomicInteger();
        StreamSpecificProcessor account = processor("t24_account_events:", accountCalls);
        StreamSpecificProcessor customer = processor("t24_customer_raw_events:", customerCalls);
        StreamProcessorRouter router = new StreamProcessorRouter(java.util.List.of(account, customer));

        router.process(record("t24_customer_raw_events:100011"));

        assertThat(accountCalls).hasValue(0);
        assertThat(customerCalls).hasValue(1);
    }

    @Test
    void rejectsAmbiguousProcessorMappings() {
        StreamProcessorRouter router = new StreamProcessorRouter(java.util.List.of(
                processor("t24_", new AtomicInteger()),
                processor("t24_customer_", new AtomicInteger())));

        assertThatThrownBy(() -> router.process(record("t24_customer_raw_events:100011")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("More than one processor");
    }

    private StreamSpecificProcessor processor(String prefix, AtomicInteger calls) {
        return new StreamSpecificProcessor() {
            @Override
            public boolean supports(String stream) {
                return stream.startsWith(prefix);
            }

            @Override
            public void process(MapRecord<String, String, String> message) {
                calls.incrementAndGet();
            }
        };
    }

    private MapRecord<String, String, String> record(String stream) {
        return StreamRecords.string(Map.of("row_id", "100011")).withStreamKey(stream);
    }
}
