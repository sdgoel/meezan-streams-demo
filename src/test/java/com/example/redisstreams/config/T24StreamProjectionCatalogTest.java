package com.example.redisstreams.config;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class T24StreamProjectionCatalogTest {

    @Test
    void acceptsOneOrManyCValuesAndRemovesDuplicates() {
        assertThat(T24StreamProjectionCatalog.normalizeCValues(List.of(" C176 ")))
                .containsExactly("c176");
        assertThat(T24StreamProjectionCatalog.normalizeCValues(
                List.of("c176", "C178", "c176")))
                .containsExactly("c176", "c178");
    }

    @Test
    void rejectsEmptyOrInvalidRequests() {
        assertThatThrownBy(() -> T24StreamProjectionCatalog.normalizeCValues(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> T24StreamProjectionCatalog.normalizeCValues(
                List.of("c176", "name")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
