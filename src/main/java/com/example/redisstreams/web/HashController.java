package com.example.redisstreams.web;

import com.example.redisstreams.service.HashService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Validated
@RestController
@RequestMapping("/api/v1/hashes/{namespace}/{id}")
public class HashController {
    private final HashService service;

    public HashController(HashService service) {
        this.service = service;
    }

    @PutMapping
    @ResponseStatus(HttpStatus.NO_CONTENT)
    void putAll(@PathVariable String namespace, @PathVariable String id,
                @Valid @RequestBody HashBody body) {
        service.putAll(namespace, id, body.fields());
    }

    @PutMapping("/fields/{field}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    void put(@PathVariable String namespace, @PathVariable String id,
             @PathVariable String field, @Valid @RequestBody ValueBody body) {
        service.put(namespace, id, field, body.value());
    }

    @GetMapping("/fields/{field}")
    ResponseEntity<Map<String, String>> get(@PathVariable String namespace, @PathVariable String id,
                                            @PathVariable String field) {
        return service.get(namespace, id, field)
                .map(value -> ResponseEntity.ok(Map.of("field", field, "value", value)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping
    Map<String, Object> getFields(@PathVariable String namespace, @PathVariable String id,
                                  @RequestParam List<String> fields) {
        return Map.of("fields", service.getFields(namespace, id, fields), "size", service.size(namespace, id));
    }

    @DeleteMapping("/fields/{field}")
    ResponseEntity<Void> deleteField(@PathVariable String namespace, @PathVariable String id,
                                     @PathVariable String field) {
        return service.deleteField(namespace, id, field)
                ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }

    @DeleteMapping
    ResponseEntity<Void> delete(@PathVariable String namespace, @PathVariable String id) {
        return service.delete(namespace, id)
                ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }

    public record HashBody(@NotEmpty Map<@NotBlank String, @NotBlank String> fields) {}
    public record ValueBody(@NotBlank String value) {}
}
