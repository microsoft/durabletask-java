// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azurefunctions;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ManagedIdentityOptions}.
 */
class ManagedIdentityOptionsTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    // ---- Constructor tests ----

    @Test
    @DisplayName("constructor: both params provided")
    void constructor_bothParams() {
        URI authority = URI.create("https://login.microsoftonline.com/");
        ManagedIdentityOptions options = new ManagedIdentityOptions(authority, "my-tenant");
        assertEquals(authority, options.getAuthorityHost());
        assertEquals("my-tenant", options.getTenantId());
    }

    @Test
    @DisplayName("constructor: null authorityHost is allowed")
    void constructor_nullAuthorityHost() {
        ManagedIdentityOptions options = new ManagedIdentityOptions(null, "tenant-id");
        assertNull(options.getAuthorityHost());
        assertEquals("tenant-id", options.getTenantId());
    }

    @Test
    @DisplayName("constructor: null tenantId is allowed")
    void constructor_nullTenantId() {
        URI authority = URI.create("https://login.microsoftonline.com/");
        ManagedIdentityOptions options = new ManagedIdentityOptions(authority, null);
        assertEquals(authority, options.getAuthorityHost());
        assertNull(options.getTenantId());
    }

    @Test
    @DisplayName("constructor: both null is allowed")
    void constructor_bothNull() {
        ManagedIdentityOptions options = new ManagedIdentityOptions(null, null);
        assertNull(options.getAuthorityHost());
        assertNull(options.getTenantId());
    }

    // ---- JSON serialization tests ----

    @Test
    @DisplayName("serialize: JSON property names are lowercase")
    void serialize_lowercasePropertyNames() throws Exception {
        ManagedIdentityOptions options = new ManagedIdentityOptions(
                URI.create("https://login.microsoftonline.com/"), "tenant-123");
        String json = mapper.writeValueAsString(options);
        assertTrue(json.contains("\"authorityhost\""));
        assertTrue(json.contains("\"tenantid\""));
        // Verify the correct values
        assertTrue(json.contains("\"authorityhost\":\"https://login.microsoftonline.com/\""));
        assertTrue(json.contains("\"tenantid\":\"tenant-123\""));
    }

    @Test
    @DisplayName("serialize: null fields are omitted")
    void serialize_nullFieldsOmitted() throws Exception {
        ManagedIdentityOptions options = new ManagedIdentityOptions(null, null);
        String json = mapper.writeValueAsString(options);
        assertFalse(json.contains("authorityhost"));
        assertFalse(json.contains("tenantid"));
    }

    @Test
    @DisplayName("serialize: partial null (only tenantId)")
    void serialize_partialNull() throws Exception {
        ManagedIdentityOptions options = new ManagedIdentityOptions(null, "my-tenant");
        String json = mapper.writeValueAsString(options);
        assertFalse(json.contains("authorityhost"));
        assertTrue(json.contains("\"tenantid\":\"my-tenant\""));
    }

    // ---- JSON deserialization tests ----

    @Test
    @DisplayName("deserialize: both fields present")
    void deserialize_bothFields() throws Exception {
        String json = "{\"authorityhost\":\"https://login.microsoftonline.com/\","
                + "\"tenantid\":\"tenant-456\"}";
        ManagedIdentityOptions options = mapper.readValue(json, ManagedIdentityOptions.class);
        assertEquals(URI.create("https://login.microsoftonline.com/"), options.getAuthorityHost());
        assertEquals("tenant-456", options.getTenantId());
    }

    @Test
    @DisplayName("deserialize: empty JSON creates instance with nulls")
    void deserialize_emptyJson() throws Exception {
        String json = "{}";
        ManagedIdentityOptions options = mapper.readValue(json, ManagedIdentityOptions.class);
        assertNull(options.getAuthorityHost());
        assertNull(options.getTenantId());
    }

    @Test
    @DisplayName("deserialize: only one field present")
    void deserialize_oneFieldPresent() throws Exception {
        String json = "{\"tenantid\":\"single-tenant\"}";
        ManagedIdentityOptions options = mapper.readValue(json, ManagedIdentityOptions.class);
        assertNull(options.getAuthorityHost());
        assertEquals("single-tenant", options.getTenantId());
    }

    // ---- Round-trip tests ----

    @Test
    @DisplayName("round-trip: with both fields")
    void roundTrip_bothFields() throws Exception {
        ManagedIdentityOptions original = new ManagedIdentityOptions(
                URI.create("https://login.microsoftonline.us/"), "us-gov-tenant");
        String json = mapper.writeValueAsString(original);
        ManagedIdentityOptions deserialized = mapper.readValue(json, ManagedIdentityOptions.class);
        assertEquals(original.getAuthorityHost(), deserialized.getAuthorityHost());
        assertEquals(original.getTenantId(), deserialized.getTenantId());
    }

    @Test
    @DisplayName("round-trip: with nulls")
    void roundTrip_nulls() throws Exception {
        ManagedIdentityOptions original = new ManagedIdentityOptions(null, null);
        String json = mapper.writeValueAsString(original);
        ManagedIdentityOptions deserialized = mapper.readValue(json, ManagedIdentityOptions.class);
        assertNull(deserialized.getAuthorityHost());
        assertNull(deserialized.getTenantId());
    }
}
