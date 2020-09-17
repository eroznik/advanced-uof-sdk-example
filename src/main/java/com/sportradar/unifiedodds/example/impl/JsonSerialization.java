package com.sportradar.unifiedodds.example.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created on 17. 09. 20
 *
 * @author e.roznik
 */
public final class JsonSerialization {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private JsonSerialization() {
        // no instance
    }

    public static String serialize(Object object) {
        try {
            return OBJECT_MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize object: " + e.getMessage(), e);
        }
    }
}
