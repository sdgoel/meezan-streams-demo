package com.example.redisstreams.domain;

import org.springframework.stereotype.Component;

import java.util.regex.Pattern;

@Component
public class KeyNames {
    private static final Pattern SAFE_SEGMENT = Pattern.compile("[a-zA-Z0-9._-]{1,100}");

    public String hash(String namespace, String id) {
        return "demo:hash:" + segment(namespace) + ":" + segment(id);
    }

    public String json(String namespace, String id) {
        return "demo:json:" + segment(namespace) + ":" + segment(id);
    }

    public String string(String namespace, String id) {
        return "demo:string:" + segment(namespace) + ":" + segment(id);
    }

    private String segment(String value) {
        if (value == null || !SAFE_SEGMENT.matcher(value).matches()) {
            throw new IllegalArgumentException("Key segments must match " + SAFE_SEGMENT.pattern());
        }
        return value.toLowerCase();
    }
}
