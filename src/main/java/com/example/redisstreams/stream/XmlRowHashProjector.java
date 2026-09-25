package com.example.redisstreams.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Component
public class XmlRowHashProjector {
    private static final Pattern SAFE_ROW_ID = Pattern.compile("[a-zA-Z0-9._-]{1,100}");

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;

    public XmlRowHashProjector(StringRedisTemplate redis, ObjectMapper objectMapper) {
        this.redis = redis;
        this.objectMapper = objectMapper;
    }

    public ProjectionResult project(String xml,
                                    String messageRowId,
                                    String keyPrefix,
                                    List<String> cValues) {
        Document document = parse(xml);
        Element row = document.getDocumentElement();
        if (!"row".equals(row.getTagName())) {
            throw new IllegalArgumentException("doc root element must be row");
        }

        String rowId = row.getAttribute("id");
        if (!SAFE_ROW_ID.matcher(rowId).matches()) {
            throw new IllegalArgumentException("doc row id is missing or invalid");
        }
        if (messageRowId != null && !messageRowId.isBlank() && !rowId.equals(messageRowId)) {
            throw new IllegalArgumentException("Stream row_id does not match doc row id");
        }

        Map<String, String> hash = new LinkedHashMap<>();
        hash.put("row_id", rowId);
        for (String cValue : cValues) {
            hash.put(cValue, extract(row, cValue));
        }

        String key = keyPrefix + "_" + rowId + "_" + String.join("_", cValues);
        redis.opsForHash().putAll(key, hash);
        return new ProjectionResult(key, Map.copyOf(hash));
    }

    private String extract(Element row, String tagName) {
        NodeList nodes = row.getElementsByTagName(tagName);
        if (nodes.getLength() == 0) {
            return "";
        }
        Element only = (Element) nodes.item(0);
        if (nodes.getLength() == 1 && !only.hasAttribute("m")) {
            return only.getTextContent();
        }

        List<Map<String, String>> values = new ArrayList<>();
        for (int index = 0; index < nodes.getLength(); index++) {
            Element element = (Element) nodes.item(index);
            Map<String, String> value = new LinkedHashMap<>();
            if (element.hasAttribute("m")) {
                value.put("m", element.getAttribute("m"));
            }
            value.put("value", element.getTextContent());
            values.add(value);
        }
        try {
            return objectMapper.writeValueAsString(values);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Could not serialize repeated XML c-values", exception);
        }
    }

    private Document parse(String xml) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
            factory.setExpandEntityReferences(false);
            return factory.newDocumentBuilder().parse(new InputSource(new StringReader(xml)));
        } catch (Exception exception) {
            throw new IllegalArgumentException("doc is not valid, safe XML", exception);
        }
    }

    public record ProjectionResult(String key, Map<String, String> fields) {
    }
}
