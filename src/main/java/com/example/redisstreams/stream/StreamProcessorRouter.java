package com.example.redisstreams.stream;

import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class StreamProcessorRouter implements StreamMessageProcessor {
    private final List<StreamSpecificProcessor> processors;
    private final LoggingStreamMessageProcessor fallback = new LoggingStreamMessageProcessor();

    public StreamProcessorRouter(List<StreamSpecificProcessor> processors) {
        this.processors = List.copyOf(processors);
    }

    @Override
    public void process(MapRecord<String, String, String> message) {
        List<StreamSpecificProcessor> matches = processors.stream()
                .filter(processor -> processor.supports(message.getStream()))
                .toList();

        if (matches.size() > 1) {
            throw new IllegalStateException(
                    "More than one processor supports Redis stream " + message.getStream());
        }
        if (matches.isEmpty()) {
            fallback.process(message);
            return;
        }
        matches.getFirst().process(message);
    }
}
