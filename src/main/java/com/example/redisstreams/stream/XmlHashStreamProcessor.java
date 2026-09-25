package com.example.redisstreams.stream;

import com.example.redisstreams.config.StreamProjectionProperties;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class XmlHashStreamProcessor implements StreamSpecificProcessor {
    private final Map<String, StreamProjectionProperties.Definition> definitions;
    private final XmlRowHashProjector projector;

    public XmlHashStreamProcessor(StreamProjectionProperties properties, XmlRowHashProjector projector) {
        this.definitions = properties.definitions().stream().collect(Collectors.toUnmodifiableMap(
                StreamProjectionProperties.Definition::stream,
                Function.identity()));
        this.projector = projector;
    }

    @Override
    public boolean supports(String stream) {
        return definitions.containsKey(stream);
    }

    @Override
    public void process(MapRecord<String, String, String> message) {
        StreamProjectionProperties.Definition definition = definitions.get(message.getStream());
        if (definition == null) {
            throw new IllegalArgumentException("No XML projection configured for " + message.getStream());
        }
        String document = message.getValue().get("doc");
        if (document == null || document.isBlank()) {
            throw new IllegalArgumentException("Stream message has no non-empty doc field");
        }
        projector.project(document, message.getValue().get("row_id"), definition);
    }
}
