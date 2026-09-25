package com.example.redisstreams.web;

import com.example.redisstreams.service.StringValueService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.Map;

@Validated
@RestController
@RequestMapping("/api/v1/strings/{namespace}/{id}")
public class StringController {
    private final StringValueService service;

    public StringController(StringValueService service) {
        this.service = service;
    }

    @PutMapping
    @ResponseStatus(HttpStatus.NO_CONTENT)
    void set(@PathVariable String namespace,
             @PathVariable String id,
             @RequestParam(required = false) @Positive Long ttlSeconds,
             @Valid @RequestBody ValueBody body) {
        Duration ttl = ttlSeconds == null ? null : Duration.ofSeconds(ttlSeconds);
        service.set(namespace, id, body.value(), ttl);
    }

    @GetMapping
    ResponseEntity<Map<String, String>> get(@PathVariable String namespace, @PathVariable String id) {
        return service.get(namespace, id)
                .map(value -> ResponseEntity.ok(Map.of("value", value)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/increment")
    Map<String, Long> increment(@PathVariable String namespace,
                                @PathVariable String id,
                                @RequestParam(defaultValue = "1") long amount) {
        return Map.of("value", service.increment(namespace, id, amount));
    }

    @DeleteMapping
    ResponseEntity<Void> delete(@PathVariable String namespace, @PathVariable String id) {
        return service.delete(namespace, id)
                ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }

    public record ValueBody(@NotNull String value) {}
}
