package com.example.redisstreams.web;

import com.example.redisstreams.service.RedisJsonService;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/json/{namespace}/{id}")
public class JsonController {
    private final RedisJsonService service;

    public JsonController(RedisJsonService service) {
        this.service = service;
    }

    @PutMapping
    @ResponseStatus(HttpStatus.NO_CONTENT)
    void set(@PathVariable String namespace, @PathVariable String id,
             @RequestParam(defaultValue = "$") String path, @RequestBody JsonNode body) {
        service.set(namespace, id, path, body);
    }

    @GetMapping
    ResponseEntity<JsonNode> get(@PathVariable String namespace, @PathVariable String id,
                                 @RequestParam(defaultValue = "$") String path) {
        return service.get(namespace, id, path).map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @DeleteMapping
    ResponseEntity<Void> delete(@PathVariable String namespace, @PathVariable String id,
                                @RequestParam(defaultValue = "$") String path) {
        return service.delete(namespace, id, path) > 0
                ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }

    @PostMapping("/increment")
    Map<String, Double> increment(@PathVariable String namespace, @PathVariable String id,
                                  @RequestParam String path, @RequestParam double amount) {
        return Map.of("value", service.increment(namespace, id, path, amount));
    }
}
