// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azurefunctions;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ManagedIdentityTokenSource}.
 */
class ManagedIdentityTokenSourceTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    // ---- Constructor tests ----

    @Test
    @DisplayName("constructor: single-arg with explicit resource")
    void constructor_singleArg() {
        ManagedIdentityTokenSource source = new ManagedIdentityTokenSource(
                "https://management.core.windows.net/.default");
        assertEquals("https://management.core.windows.net/.default", source.getResource());
        assertNull(source.getOptions());
    }

    @Test
    @DisplayName("constructor: with options")
    void constructor_withOptions() {
        ManagedIdentityOptions options = new ManagedIdentityOptions(
                URI.create("https://login.microsoftonline.com/"), "my-tenant");
        ManagedIdentityTokenSource source = new ManagedIdentityTokenSource(
                "https://graph.microsoft.com/.default", options);
        assertEquals("https://graph.microsoft.com/.default", source.getResource());
        assertNotNull(source.getOptions());
        assertEquals("my-tenant", source.getOptions().getTenantId());
    }

    @Test
    @DisplayName("constructor: null options is allowed")
    void constructor_nullOptions() {
        ManagedIdentityTokenSource source = new ManagedIdentityTokenSource(
                "https://custom.resource", null);
        assertEquals("https://custom.resource", source.getResource());
        assertNull(source.getOptions());
    }

    @Test
    @DisplayName("constructor: null resource throws IllegalArgumentException")
    void constructor_nullResource_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> new ManagedIdentityTokenSource(null));
    }

    @Test
    @DisplayName("constructor: empty resource throws IllegalArgumentException")
    void constructor_emptyResource_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> new ManagedIdentityTokenSource("  "));
    }

    // ---- Auto-normalization tests ----

    @Test
    @DisplayName("auto-normalization: management.core.windows.net gets /.default appended")
    void autoNormalize_managementUri() {
        ManagedIdentityTokenSource source = new ManagedIdentityTokenSource(
                "https://management.core.windows.net");
        assertEquals("https://management.core.windows.net/.default", source.getResource());
    }

    @Test
    @DisplayName("auto-normalization: graph.microsoft.com gets /.default appended")
    void autoNormalize_graphUri() {
        ManagedIdentityTokenSource source = new ManagedIdentityTokenSource(
                "https://graph.microsoft.com");
        assertEquals("https://graph.microsoft.com/.default", source.getResource());
    }

    @Test
    @DisplayName("auto-normalization: already has /.default — not doubled")
    void autoNormalize_alreadyHasDefault() {
        ManagedIdentityTokenSource source = new ManagedIdentityTokenSource(
                "https://management.core.windows.net/.default");
        assertEquals("https://management.core.windows.net/.default", source.getResource());
    }

    @Test
    @DisplayName("auto-normalization: custom resource URI is NOT normalized")
    void autoNormalize_customUri_unchanged() {
        ManagedIdentityTokenSource source = new ManagedIdentityTokenSource(
                "https://my-custom-api.com");
        assertEquals("https://my-custom-api.com", source.getResource());
    }

    // ---- Kind tests ----

    @Test
    @DisplayName("getKind: returns 'AzureManagedIdentity'")
    void getKind() {
        ManagedIdentityTokenSource source = new ManagedIdentityTokenSource(
                "https://management.core.windows.net/.default");
        assertEquals("AzureManagedIdentity", source.getKind());
    }

    // ---- JSON serialization tests ----

    @Test
    @DisplayName("serialize: includes kind and resource")
    void serialize_basic() throws Exception {
        ManagedIdentityTokenSource source = new ManagedIdentityTokenSource(
                "https://management.core.windows.net/.default");
        String json = mapper.writeValueAsString(source);
        assertTrue(json.contains("\"kind\":\"AzureManagedIdentity\""));
        assertTrue(json.contains("\"resource\":\"https://management.core.windows.net/.default\""));
        assertFalse(json.contains("\"options\"")); // null options omitted
    }

    @Test
    @DisplayName("serialize: includes options when present")
    void serialize_withOptions() throws Exception {
        ManagedIdentityOptions options = new ManagedIdentityOptions(
                URI.create("https://login.microsoftonline.com/"), "tenant-123");
        ManagedIdentityTokenSource source = new ManagedIdentityTokenSource(
                "https://management.core.windows.net/.default", options);
        String json = mapper.writeValueAsString(source);
        assertTrue(json.contains("\"options\""));
        assertTrue(json.contains("\"authorityhost\":\"https://login.microsoftonline.com/\""));
        assertTrue(json.contains("\"tenantid\":\"tenant-123\""));
    }

    // ---- JSON deserialization tests ----

    @Test
    @DisplayName("deserialize: basic token source")
    void deserialize_basic() throws Exception {
        String json = "{\"kind\":\"AzureManagedIdentity\",\"resource\":\"https://custom.api/.default\"}";
        ManagedIdentityTokenSource source = mapper.readValue(json, ManagedIdentityTokenSource.class);
        assertEquals("AzureManagedIdentity", source.getKind());
        assertEquals("https://custom.api/.default", source.getResource());
        assertNull(source.getOptions());
    }

    @Test
    @DisplayName("deserialize: with options")
    void deserialize_withOptions() throws Exception {
        String json = "{\"kind\":\"AzureManagedIdentity\","
                + "\"resource\":\"https://management.core.windows.net/.default\","
                + "\"options\":{\"authorityhost\":\"https://login.microsoftonline.com/\","
                + "\"tenantid\":\"my-tenant\"}}";
        ManagedIdentityTokenSource source = mapper.readValue(json, ManagedIdentityTokenSource.class);
        assertNotNull(source.getOptions());
        assertEquals(URI.create("https://login.microsoftonline.com/"),
                source.getOptions().getAuthorityHost());
        assertEquals("my-tenant", source.getOptions().getTenantId());
    }

    // ---- Polymorphic deserialization tests ----

    @Test
    @DisplayName("polymorphic deserialize: TokenSource base type resolves to ManagedIdentityTokenSource")
    void polymorphicDeserialize() throws Exception {
        String json = "{\"kind\":\"AzureManagedIdentity\","
                + "\"resource\":\"https://management.core.windows.net/.default\"}";
        TokenSource source = mapper.readValue(json, TokenSource.class);
        assertInstanceOf(ManagedIdentityTokenSource.class, source);
        assertEquals("https://management.core.windows.net/.default",
                ((ManagedIdentityTokenSource) source).getResource());
    }

    // ---- Round-trip tests ----

    @Test
    @DisplayName("round-trip: basic token source")
    void roundTrip_basic() throws Exception {
        ManagedIdentityTokenSource original = new ManagedIdentityTokenSource(
                "https://management.core.windows.net/.default");
        String json = mapper.writeValueAsString(original);
        ManagedIdentityTokenSource deserialized = mapper.readValue(json, ManagedIdentityTokenSource.class);
        assertEquals(original.getKind(), deserialized.getKind());
        assertEquals(original.getResource(), deserialized.getResource());
    }

    @Test
    @DisplayName("round-trip: token source with options")
    void roundTrip_withOptions() throws Exception {
        ManagedIdentityOptions options = new ManagedIdentityOptions(
                URI.create("https://login.microsoftonline.us/"), "us-gov-tenant");
        ManagedIdentityTokenSource original = new ManagedIdentityTokenSource(
                "https://management.core.windows.net/.default", options);
        String json = mapper.writeValueAsString(original);
        ManagedIdentityTokenSource deserialized = mapper.readValue(json, ManagedIdentityTokenSource.class);
        assertEquals(original.getResource(), deserialized.getResource());
        assertNotNull(deserialized.getOptions());
        assertEquals(original.getOptions().getAuthorityHost(), deserialized.getOptions().getAuthorityHost());
        assertEquals(original.getOptions().getTenantId(), deserialized.getOptions().getTenantId());
    }

    // ---- Polymorphic round-trip tests ----

    @Test
    @DisplayName("polymorphic round-trip: serialize as TokenSource, deserialize as TokenSource")
    void polymorphicRoundTrip() throws Exception {
        TokenSource original = new ManagedIdentityTokenSource(
                "https://graph.microsoft.com/.default");
        String json = mapper.writeValueAsString(original);
        TokenSource deserialized = mapper.readValue(json, TokenSource.class);
        assertInstanceOf(ManagedIdentityTokenSource.class, deserialized);
        assertEquals("AzureManagedIdentity", deserialized.getKind());
        assertEquals("https://graph.microsoft.com/.default",
                ((ManagedIdentityTokenSource) deserialized).getResource());
    }
}
