package com.example.redisstreams.stream;

import com.example.redisstreams.config.StreamProjectionProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class XmlRowHashProjectorTest {
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final HashOperations<String, Object, Object> hashes = mock(HashOperations.class);
    private final XmlRowHashProjector projector = new XmlRowHashProjector(redis, new ObjectMapper());

    @Test
    void createsHashKeyAndPreservesRepeatedCValues() {
        when(redis.opsForHash()).thenReturn(hashes);
        String xml = "<row id='100011'><c176>ACTIVE</c176>"
                + "<c178 m='12'>ELAHI BUKHSH</c178><c178 m='13'>UMER JAHAN</c178></row>";
        var definition = new StreamProjectionProperties.Definition(
                "t24_customer_events", "t24_customer", List.of("c176", "c178"));

        XmlRowHashProjector.ProjectionResult result = projector.project(xml, "100011", definition);

        assertThat(result.key()).isEqualTo("t24_customer_100011_c176_c178");
        assertThat(result.fields()).containsEntry("row_id", "100011").containsEntry("c176", "ACTIVE");
        assertThat(result.fields().get("c178"))
                .isEqualTo("[{\"m\":\"12\",\"value\":\"ELAHI BUKHSH\"},{\"m\":\"13\",\"value\":\"UMER JAHAN\"}]");
        verify(hashes).putAll(result.key(), result.fields());
    }

    @Test
    void storesEmptyStringWhenAConfiguredCValueIsAbsent() {
        when(redis.opsForHash()).thenReturn(hashes);
        var definition = new StreamProjectionProperties.Definition(
                "t24_customer_events", "t24_customer", List.of("c176", "c178"));

        XmlRowHashProjector.ProjectionResult result = projector.project(
                "<row id='100011'><c178>present</c178></row>", null, definition);

        assertThat(result.fields()).containsEntry("c176", "").containsEntry("c178", "present");
    }

    @Test
    void rejectsExternalEntitiesAndMismatchedRowIds() {
        var definition = new StreamProjectionProperties.Definition(
                "t24_customer_events", "t24_customer", List.of("c178"));

        assertThatThrownBy(() -> projector.project(
                "<!DOCTYPE row [<!ENTITY xxe SYSTEM 'file:///etc/passwd'>]><row id='1'><c178>&xxe;</c178></row>",
                null, definition)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> projector.project(
                "<row id='100011'><c178>value</c178></row>", "different", definition))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not match");
    }
}
