package com.example.redisstreams.stream;

import com.example.redisstreams.config.T24StreamProjectionCatalog;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class XmlHashStreamProcessor implements StreamSpecificProcessor {
    private final T24StreamProjectionCatalog catalog;
    private final XmlRowHashProjector projector;

    public XmlHashStreamProcessor(T24StreamProjectionCatalog catalog, XmlRowHashProjector projector) {
        this.catalog = catalog;
        this.projector = projector;
    }

    @Override
    public boolean supports(String stream) {
        return catalog.find(stream).isPresent();
    }

    @Override
    public void process(MapRecord<String, String, String> message) {
        T24StreamProjectionCatalog.Definition definition = catalog.find(message.getStream())
                .orElseThrow(() -> new IllegalArgumentException(
                        "No XML projection configured for " + message.getStream()));
        String document = message.getValue().get("doc");
        if (document == null || document.isBlank()) {
            throw new IllegalArgumentException("Stream message has no non-empty doc field");
        }
        List<String> requestedCValues = requestedCValues(
                message.getValue().get("c_values"), definition.defaultCValues());
        projector.project(document, message.getValue().get("row_id"),
                definition.keyPrefix(), requestedCValues);
    }

    private List<String> requestedCValues(String requested, List<String> defaults) {
        if (requested == null || requested.isBlank()) {
            return defaults;
        }
        return T24StreamProjectionCatalog.normalizeCValues(Arrays.asList(requested.split(",")));
    }
}
