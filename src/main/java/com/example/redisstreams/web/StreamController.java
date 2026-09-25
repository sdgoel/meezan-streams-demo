package com.example.redisstreams.web;

import com.example.redisstreams.service.StreamPublisher;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/streams")
public class StreamController {
    private final StreamPublisher publisher;

    public StreamController(StreamPublisher publisher) {
        this.publisher = publisher;
    }

    @PostMapping("/{stream}/messages")
    @ResponseStatus(HttpStatus.CREATED)
    Map<String, String> publish(@PathVariable String stream, @Valid @RequestBody MessageBody body) {
        return Map.of("id", publisher.publish(stream, body.fields()).getValue(), "stream", stream);
    }

    public record MessageBody(@NotEmpty Map<@NotBlank String, @NotBlank String> fields) {}
}
