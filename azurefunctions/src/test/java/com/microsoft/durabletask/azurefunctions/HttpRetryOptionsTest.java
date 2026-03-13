// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azurefunctions;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link HttpRetryOptions}.
 */
class HttpRetryOptionsTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    // ---- Public constructor validation tests ----

    @Test
    @DisplayName("constructor: valid params create instance with defaults")
    void constructor_validParams() {
        HttpRetryOptions options = new HttpRetryOptions(Duration.ofSeconds(5), 3);
        assertEquals(Duration.ofSeconds(5), options.getFirstRetryInterval());
        assertEquals(3, options.getMaxNumberOfAttempts());
        // defaults
        assertEquals(Duration.ofDays(6), options.getMaxRetryInterval());
        assertEquals(1.0, options.getBackoffCoefficient());
        assertNull(options.getRetryTimeout());
        assertTrue(options.getStatusCodesToRetry().isEmpty());
    }

    @Test
    @DisplayName("constructor: null firstRetryInterval throws")
    void constructor_nullInterval_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> new HttpRetryOptions(null, 3));
    }

    @Test
    @DisplayName("constructor: zero firstRetryInterval throws")
    void constructor_zeroInterval_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> new HttpRetryOptions(Duration.ZERO, 3));
    }

    @Test
    @DisplayName("constructor: negative firstRetryInterval throws")
    void constructor_negativeInterval_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> new HttpRetryOptions(Duration.ofSeconds(-1), 3));
    }

    @Test
    @DisplayName("constructor: maxNumberOfAttempts < 1 throws")
    void constructor_zeroAttempts_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> new HttpRetryOptions(Duration.ofSeconds(5), 0));
    }

    @Test
    @DisplayName("constructor: maxNumberOfAttempts = 1 is valid (no retry)")
    void constructor_oneAttempt() {
        HttpRetryOptions options = new HttpRetryOptions(Duration.ofSeconds(1), 1);
        assertEquals(1, options.getMaxNumberOfAttempts());
    }

    // ---- Setter tests ----

    @Test
    @DisplayName("setMaxRetryInterval: updates value")
    void setMaxRetryInterval() {
        HttpRetryOptions options = new HttpRetryOptions(Duration.ofSeconds(5), 3);
        options.setMaxRetryInterval(Duration.ofMinutes(10));
        assertEquals(Duration.ofMinutes(10), options.getMaxRetryInterval());
    }

    @Test
    @DisplayName("setBackoffCoefficient: updates value")
    void setBackoffCoefficient() {
        HttpRetryOptions options = new HttpRetryOptions(Duration.ofSeconds(5), 3);
        options.setBackoffCoefficient(2.5);
        assertEquals(2.5, options.getBackoffCoefficient());
    }

    @Test
    @DisplayName("setRetryTimeout: updates value")
    void setRetryTimeout() {
        HttpRetryOptions options = new HttpRetryOptions(Duration.ofSeconds(5), 3);
        options.setRetryTimeout(Duration.ofMinutes(30));
        assertEquals(Duration.ofMinutes(30), options.getRetryTimeout());
    }

    @Test
    @DisplayName("setRetryTimeout: null clears timeout")
    void setRetryTimeout_null() {
        HttpRetryOptions options = new HttpRetryOptions(Duration.ofSeconds(5), 3);
        options.setRetryTimeout(Duration.ofMinutes(30));
        options.setRetryTimeout(null);
        assertNull(options.getRetryTimeout());
    }

    @Test
    @DisplayName("setStatusCodesToRetry: updates value")
    void setStatusCodesToRetry() {
        HttpRetryOptions options = new HttpRetryOptions(Duration.ofSeconds(5), 3);
        options.setStatusCodesToRetry(Arrays.asList(500, 502, 503));
        assertEquals(Arrays.asList(500, 502, 503), options.getStatusCodesToRetry());
    }

    @Test
    @DisplayName("setStatusCodesToRetry: null resets to empty")
    void setStatusCodesToRetry_null() {
        HttpRetryOptions options = new HttpRetryOptions(Duration.ofSeconds(5), 3);
        options.setStatusCodesToRetry(Arrays.asList(500, 502));
        options.setStatusCodesToRetry(null);
        assertTrue(options.getStatusCodesToRetry().isEmpty());
    }

    @Test
    @DisplayName("setStatusCodesToRetry: defensive copy — original list modification does not affect options")
    void setStatusCodesToRetry_defensiveCopy() {
        HttpRetryOptions options = new HttpRetryOptions(Duration.ofSeconds(5), 3);
        List<Integer> codes = new java.util.ArrayList<>(Arrays.asList(500, 502));
        options.setStatusCodesToRetry(codes);
        codes.add(503); // modify original
        assertEquals(2, options.getStatusCodesToRetry().size()); // should be unaffected
    }

    @Test
    @DisplayName("getStatusCodesToRetry: returns unmodifiable list")
    void getStatusCodesToRetry_unmodifiable() {
        HttpRetryOptions options = new HttpRetryOptions(Duration.ofSeconds(5), 3);
        options.setStatusCodesToRetry(Arrays.asList(500));
        assertThrows(UnsupportedOperationException.class,
                () -> options.getStatusCodesToRetry().add(502));
    }

    // ---- JSON serialization tests ----

    @Test
    @DisplayName("serialize: basic options with TimeSpan format")
    void serialize_basic() throws Exception {
        HttpRetryOptions options = new HttpRetryOptions(Duration.ofSeconds(5), 3);
        String json = mapper.writeValueAsString(options);
        // firstRetryInterval should be TimeSpan format: "00:00:05"
        assertTrue(json.contains("\"firstRetryInterval\":\"00:00:05\""), "Got: " + json);
        assertTrue(json.contains("\"maxNumberOfAttempts\":3"), "Got: " + json);
        assertTrue(json.contains("\"backoffCoefficient\":1.0"), "Got: " + json);
        // maxRetryInterval = 6 days = "6.00:00:00"
        assertTrue(json.contains("\"maxRetryInterval\":\"6.00:00:00\""), "Got: " + json);
        // retryTimeout is null — should not be present
        assertFalse(json.contains("\"retryTimeout\""), "Got: " + json);
    }

    @Test
    @DisplayName("serialize: with all optional fields set")
    void serialize_allFields() throws Exception {
        HttpRetryOptions options = new HttpRetryOptions(Duration.ofSeconds(10), 5);
        options.setMaxRetryInterval(Duration.ofMinutes(5));
        options.setBackoffCoefficient(2.0);
        options.setRetryTimeout(Duration.ofHours(1));
        options.setStatusCodesToRetry(Arrays.asList(500, 502, 503));
        String json = mapper.writeValueAsString(options);
        assertTrue(json.contains("\"firstRetryInterval\":\"00:00:10\""), json);
        assertTrue(json.contains("\"maxNumberOfAttempts\":5"), json);
        assertTrue(json.contains("\"maxRetryInterval\":\"00:05:00\""), json);
        assertTrue(json.contains("\"backoffCoefficient\":2.0"), json);
        assertTrue(json.contains("\"retryTimeout\":\"01:00:00\""), json);
        assertTrue(json.contains("\"statusCodesToRetry\":[500,502,503]"), json);
    }

    @Test
    @DisplayName("serialize: empty statusCodesToRetry is included")
    void serialize_emptyStatusCodes() throws Exception {
        HttpRetryOptions options = new HttpRetryOptions(Duration.ofSeconds(5), 3);
        String json = mapper.writeValueAsString(options);
        // empty list should be serialized
        assertTrue(json.contains("\"statusCodesToRetry\":[]"), "Got: " + json);
    }

    // ---- JSON deserialization tests ----

    @Test
    @DisplayName("deserialize: basic options from TimeSpan strings")
    void deserialize_basic() throws Exception {
        String json = "{\"firstRetryInterval\":\"00:00:05\",\"maxNumberOfAttempts\":3}";
        HttpRetryOptions options = mapper.readValue(json, HttpRetryOptions.class);
        assertEquals(Duration.ofSeconds(5), options.getFirstRetryInterval());
        assertEquals(3, options.getMaxNumberOfAttempts());
        // defaults applied
        assertEquals(Duration.ofDays(6), options.getMaxRetryInterval());
        assertEquals(1.0, options.getBackoffCoefficient());
        assertNull(options.getRetryTimeout());
        assertTrue(options.getStatusCodesToRetry().isEmpty());
    }

    @Test
    @DisplayName("deserialize: all fields present")
    void deserialize_allFields() throws Exception {
        String json = "{"
                + "\"firstRetryInterval\":\"00:00:10\","
                + "\"maxNumberOfAttempts\":5,"
                + "\"maxRetryInterval\":\"00:05:00\","
                + "\"backoffCoefficient\":2.0,"
                + "\"retryTimeout\":\"01:00:00\","
                + "\"statusCodesToRetry\":[500,502,503]"
                + "}";
        HttpRetryOptions options = mapper.readValue(json, HttpRetryOptions.class);
        assertEquals(Duration.ofSeconds(10), options.getFirstRetryInterval());
        assertEquals(5, options.getMaxNumberOfAttempts());
        assertEquals(Duration.ofMinutes(5), options.getMaxRetryInterval());
        assertEquals(2.0, options.getBackoffCoefficient());
        assertEquals(Duration.ofHours(1), options.getRetryTimeout());
        assertEquals(Arrays.asList(500, 502, 503), options.getStatusCodesToRetry());
    }

    @Test
    @DisplayName("deserialize: empty JSON uses defaults via @JsonCreator")
    void deserialize_emptyJson() throws Exception {
        String json = "{}";
        HttpRetryOptions options = mapper.readValue(json, HttpRetryOptions.class);
        // null firstRetryInterval because not provided
        assertNull(options.getFirstRetryInterval());
        assertEquals(0, options.getMaxNumberOfAttempts());
        assertEquals(Duration.ofDays(6), options.getMaxRetryInterval());
        assertEquals(1.0, options.getBackoffCoefficient());
        assertNull(options.getRetryTimeout());
        assertTrue(options.getStatusCodesToRetry().isEmpty());
    }

    @Test
    @DisplayName("deserialize: TimeSpan with days")
    void deserialize_withDays() throws Exception {
        String json = "{\"firstRetryInterval\":\"1.00:00:00\",\"maxNumberOfAttempts\":1}";
        HttpRetryOptions options = mapper.readValue(json, HttpRetryOptions.class);
        assertEquals(Duration.ofDays(1), options.getFirstRetryInterval());
    }

    // ---- Round-trip tests ----

    @Test
    @DisplayName("round-trip: basic options")
    void roundTrip_basic() throws Exception {
        HttpRetryOptions original = new HttpRetryOptions(Duration.ofSeconds(30), 5);
        String json = mapper.writeValueAsString(original);
        HttpRetryOptions deserialized = mapper.readValue(json, HttpRetryOptions.class);
        assertEquals(original.getFirstRetryInterval(), deserialized.getFirstRetryInterval());
        assertEquals(original.getMaxNumberOfAttempts(), deserialized.getMaxNumberOfAttempts());
        assertEquals(original.getMaxRetryInterval(), deserialized.getMaxRetryInterval());
        assertEquals(original.getBackoffCoefficient(), deserialized.getBackoffCoefficient());
        assertEquals(original.getRetryTimeout(), deserialized.getRetryTimeout());
        assertEquals(original.getStatusCodesToRetry(), deserialized.getStatusCodesToRetry());
    }

    @Test
    @DisplayName("round-trip: all fields set")
    void roundTrip_allFields() throws Exception {
        HttpRetryOptions original = new HttpRetryOptions(Duration.ofMillis(500), 10);
        original.setMaxRetryInterval(Duration.ofMinutes(30));
        original.setBackoffCoefficient(1.5);
        original.setRetryTimeout(Duration.ofHours(2));
        original.setStatusCodesToRetry(Arrays.asList(429, 500, 502, 503, 504));
        String json = mapper.writeValueAsString(original);
        HttpRetryOptions deserialized = mapper.readValue(json, HttpRetryOptions.class);
        assertEquals(original.getFirstRetryInterval(), deserialized.getFirstRetryInterval());
        assertEquals(original.getMaxNumberOfAttempts(), deserialized.getMaxNumberOfAttempts());
        assertEquals(original.getMaxRetryInterval(), deserialized.getMaxRetryInterval());
        assertEquals(original.getBackoffCoefficient(), deserialized.getBackoffCoefficient());
        assertEquals(original.getRetryTimeout(), deserialized.getRetryTimeout());
        assertEquals(original.getStatusCodesToRetry(), deserialized.getStatusCodesToRetry());
    }
}
