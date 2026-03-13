// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azurefunctions;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link DurableHttpResponse}.
 */
class DurableHttpResponseTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    // ---- Constructor tests ----

    @Test
    @DisplayName("constructor: creates response with all fields")
    void constructor_allFields() {
        Map<String, String> headers = Collections.singletonMap("Content-Type", "application/json");
        DurableHttpResponse response = new DurableHttpResponse(200, headers, "{\"ok\":true}");
        assertEquals(200, response.getStatusCode());
        assertEquals("application/json", response.getHeaders().get("Content-Type"));
        assertEquals("{\"ok\":true}", response.getContent());
    }

    @Test
    @DisplayName("constructor: null headers and content are allowed")
    void constructor_nullHeadersAndContent() {
        DurableHttpResponse response = new DurableHttpResponse(404, null, null);
        assertEquals(404, response.getStatusCode());
        assertNull(response.getHeaders());
        assertNull(response.getContent());
    }

    @Test
    @DisplayName("constructor: 500 status code is valid")
    void constructor_errorStatusCode() {
        DurableHttpResponse response = new DurableHttpResponse(500, null, "Internal Server Error");
        assertEquals(500, response.getStatusCode());
        assertEquals("Internal Server Error", response.getContent());
    }

    @Test
    @DisplayName("headers are immutable")
    void headers_immutable() {
        Map<String, String> headers = new HashMap<>();
        headers.put("key", "value");
        DurableHttpResponse response = new DurableHttpResponse(200, headers, null);
        assertThrows(UnsupportedOperationException.class,
                () -> response.getHeaders().put("new", "value"));
    }

    // ---- JSON serialization tests ----

    @Test
    @DisplayName("serialize: includes all present fields")
    void serialize_allFields() throws Exception {
        Map<String, String> headers = Collections.singletonMap("X-Custom", "test");
        DurableHttpResponse response = new DurableHttpResponse(200, headers, "body");
        String json = mapper.writeValueAsString(response);
        assertTrue(json.contains("\"statusCode\":200"));
        assertTrue(json.contains("\"X-Custom\":\"test\""));
        assertTrue(json.contains("\"content\":\"body\""));
    }

    @Test
    @DisplayName("serialize: null fields omitted with NON_NULL")
    void serialize_nullFieldsOmitted() throws Exception {
        DurableHttpResponse response = new DurableHttpResponse(204, null, null);
        String json = mapper.writeValueAsString(response);
        assertTrue(json.contains("\"statusCode\":204"));
        assertFalse(json.contains("\"headers\""));
        assertFalse(json.contains("\"content\""));
    }

    // ---- JSON deserialization tests ----

    @Test
    @DisplayName("deserialize: minimal response with status code only")
    void deserialize_minimal() throws Exception {
        String json = "{\"statusCode\":200}";
        DurableHttpResponse response = mapper.readValue(json, DurableHttpResponse.class);
        assertEquals(200, response.getStatusCode());
        assertNull(response.getHeaders());
        assertNull(response.getContent());
    }

    @Test
    @DisplayName("deserialize: full response with all fields")
    void deserialize_allFields() throws Exception {
        String json = "{\"statusCode\":201,\"headers\":{\"Location\":\"/items/1\"},\"content\":\"{\\\"id\\\":1}\"}";
        DurableHttpResponse response = mapper.readValue(json, DurableHttpResponse.class);
        assertEquals(201, response.getStatusCode());
        assertEquals("/items/1", response.getHeaders().get("Location"));
        assertEquals("{\"id\":1}", response.getContent());
    }

    // ---- Round-trip tests ----

    @Test
    @DisplayName("round-trip: serialized then deserialized response is equivalent")
    void roundTrip() throws Exception {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("X-Request-Id", "abc123");
        DurableHttpResponse original = new DurableHttpResponse(200, headers, "{\"data\":\"test\"}");

        String json = mapper.writeValueAsString(original);
        DurableHttpResponse deserialized = mapper.readValue(json, DurableHttpResponse.class);

        assertEquals(original.getStatusCode(), deserialized.getStatusCode());
        assertEquals(original.getHeaders(), deserialized.getHeaders());
        assertEquals(original.getContent(), deserialized.getContent());
    }

    @Test
    @DisplayName("round-trip: response with null fields")
    void roundTrip_nullFields() throws Exception {
        DurableHttpResponse original = new DurableHttpResponse(204, null, null);
        String json = mapper.writeValueAsString(original);
        DurableHttpResponse deserialized = mapper.readValue(json, DurableHttpResponse.class);
        assertEquals(original.getStatusCode(), deserialized.getStatusCode());
        assertNull(deserialized.getHeaders());
        assertNull(deserialized.getContent());
    }
}
