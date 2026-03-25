// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link DurableHttpRequest}.
 */
class DurableHttpRequestTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    // ---- Constructor tests ----

    @Test
    void constructorMinimal() {
        DurableHttpRequest req = new DurableHttpRequest("GET", URI.create("https://example.com"));
        assertEquals("GET", req.getMethod());
        assertEquals(URI.create("https://example.com"), req.getUri());
        assertNull(req.getHeaders());
        assertNull(req.getContent());
        assertNull(req.getTokenSource());
        assertTrue(req.isAsynchronousPatternEnabled());
        assertNull(req.getTimeout());
        assertNull(req.getHttpRetryOptions());
    }

    @Test
    void constructorWithHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        DurableHttpRequest req = new DurableHttpRequest("POST", URI.create("https://example.com"), headers);
        assertEquals("POST", req.getMethod());
        assertEquals("application/json", req.getHeaders().get("Content-Type"));
    }

    @Test
    void constructorWithHeadersAndContent() {
        Map<String, String> headers = Collections.singletonMap("Accept", "text/plain");
        DurableHttpRequest req = new DurableHttpRequest("PUT", URI.create("https://example.com"), headers, "body");
        assertEquals("PUT", req.getMethod());
        assertEquals("body", req.getContent());
        assertEquals("text/plain", req.getHeaders().get("Accept"));
    }

    @Test
    void constructorWithTokenSource() {
        TokenSource token = new ManagedIdentityTokenSource("https://graph.microsoft.com/.default");
        DurableHttpRequest req = new DurableHttpRequest("GET", URI.create("https://example.com"),
                null, null, token);
        assertNotNull(req.getTokenSource());
        assertTrue(req.isAsynchronousPatternEnabled());
    }

    @Test
    void constructorWithAsyncPatternDisabled() {
        DurableHttpRequest req = new DurableHttpRequest("GET", URI.create("https://example.com"),
                null, null, null, false);
        assertFalse(req.isAsynchronousPatternEnabled());
    }

    @Test
    void constructorWithTimeoutAndRetry() {
        HttpRetryOptions retry = new HttpRetryOptions(Duration.ofSeconds(5), 3);
        DurableHttpRequest req = new DurableHttpRequest("GET", URI.create("https://example.com"),
                null, null, null, true, (Duration) Duration.ofMinutes(10), retry);
        assertEquals(Duration.ofMinutes(10), req.getTimeout());
        assertNotNull(req.getHttpRetryOptions());
        assertEquals(3, req.getHttpRetryOptions().getMaxNumberOfAttempts());
    }

    @Test
    void constructorWithNullTimeout() {
        DurableHttpRequest req = new DurableHttpRequest("GET", URI.create("https://example.com"),
                null, null, null, true, (Duration) null, null);
        assertNull(req.getTimeout());
        assertNull(req.getHttpRetryOptions());
    }

    @Test
    void constructorNullMethodThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> new DurableHttpRequest(null, URI.create("https://example.com")));
    }

    @Test
    void constructorEmptyMethodThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> new DurableHttpRequest("  ", URI.create("https://example.com")));
    }

    @Test
    void constructorNullUriThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> new DurableHttpRequest("GET", null));
    }

    @Test
    void headersAreImmutable() {
        Map<String, String> headers = new HashMap<>();
        headers.put("key", "value");
        DurableHttpRequest req = new DurableHttpRequest("GET", URI.create("https://example.com"), headers);
        assertThrows(UnsupportedOperationException.class,
                () -> req.getHeaders().put("newKey", "newValue"));
    }

    // ---- JSON serialization tests ----

    @Test
    void serializeMinimalRequest() throws Exception {
        DurableHttpRequest req = new DurableHttpRequest("GET", URI.create("https://example.com"));
        String json = mapper.writeValueAsString(req);
        assertTrue(json.contains("\"method\":\"GET\""));
        assertTrue(json.contains("\"uri\":\"https://example.com\""));
        assertTrue(json.contains("\"asynchronousPatternEnabled\":true"));
        // null fields should not appear due to NON_NULL
        assertFalse(json.contains("\"headers\""));
        assertFalse(json.contains("\"content\""));
        assertFalse(json.contains("\"tokenSource\""));
        assertFalse(json.contains("\"timeout\""));
        assertFalse(json.contains("\"retryOptions\""));
    }

    @Test
    void serializeWithTimeout() throws Exception {
        DurableHttpRequest req = new DurableHttpRequest("GET", URI.create("https://example.com"),
                null, null, null, true, Duration.ofMinutes(5), null);
        String json = mapper.writeValueAsString(req);
        assertTrue(json.contains("\"timeout\":\"00:05:00\""));
    }

    @Test
    void serializeWithRetryOptions() throws Exception {
        HttpRetryOptions retry = new HttpRetryOptions(Duration.ofSeconds(5), 3);
        DurableHttpRequest req = new DurableHttpRequest("POST", URI.create("https://example.com"),
                null, "{}", null, true, (Duration) null, retry);
        String json = mapper.writeValueAsString(req);
        assertTrue(json.contains("\"retryOptions\""));
        assertTrue(json.contains("\"maxNumberOfAttempts\":3"));
        assertTrue(json.contains("\"firstRetryInterval\":\"00:00:05\""));
    }

    @Test
    void serializeWithAllFields() throws Exception {
        Map<String, String> headers = Collections.singletonMap("Authorization", "Bearer token");
        TokenSource token = new ManagedIdentityTokenSource("https://management.core.windows.net/.default");
        HttpRetryOptions retry = new HttpRetryOptions(Duration.ofSeconds(1), 5);
        retry.setStatusCodesToRetry(Arrays.asList(500, 502));

        DurableHttpRequest req = new DurableHttpRequest("POST", URI.create("https://api.example.com"),
                headers, "request-body", token, false, Duration.ofMinutes(30), retry);

        String json = mapper.writeValueAsString(req);
        assertTrue(json.contains("\"method\":\"POST\""));
        assertTrue(json.contains("\"uri\":\"https://api.example.com\""));
        assertTrue(json.contains("\"Authorization\":\"Bearer token\""));
        assertTrue(json.contains("\"content\":\"request-body\""));
        assertTrue(json.contains("\"tokenSource\""));
        assertTrue(json.contains("\"asynchronousPatternEnabled\":false"));
        assertTrue(json.contains("\"timeout\":\"00:30:00\""));
        assertTrue(json.contains("\"retryOptions\""));
    }

    @Test
    void serializeWithTokenSourceIncludesKindAndResource() throws Exception {
        TokenSource token = new ManagedIdentityTokenSource("https://management.core.windows.net/.default");
        DurableHttpRequest req = new DurableHttpRequest("GET", URI.create("https://example.com"),
                null, null, token);
        String json = mapper.writeValueAsString(req);
        assertTrue(json.contains("\"kind\":\"AzureManagedIdentity\""));
        assertTrue(json.contains("\"resource\":\"https://management.core.windows.net/.default\""));
    }

    // ---- JSON deserialization tests ----

    @Test
    @DisplayName("deserialize: omitted asynchronousPatternEnabled defaults to true")
    void deserializeWithoutAsyncPattern_defaultsToTrue() throws Exception {
        String json = "{\"method\":\"GET\",\"uri\":\"https://example.com\"}";
        DurableHttpRequest req = mapper.readValue(json, DurableHttpRequest.class);
        assertTrue(req.isAsynchronousPatternEnabled());
    }

    @Test
    void deserializeMinimalRequest() throws Exception {
        String json = "{\"method\":\"GET\",\"uri\":\"https://example.com\",\"asynchronousPatternEnabled\":true}";
        DurableHttpRequest req = mapper.readValue(json, DurableHttpRequest.class);
        assertEquals("GET", req.getMethod());
        assertEquals(URI.create("https://example.com"), req.getUri());
        assertTrue(req.isAsynchronousPatternEnabled());
        assertNull(req.getTimeout());
        assertNull(req.getHttpRetryOptions());
    }

    @Test
    void deserializeWithTimeout() throws Exception {
        String json = "{\"method\":\"GET\",\"uri\":\"https://example.com\","
                + "\"asynchronousPatternEnabled\":true,\"timeout\":\"00:05:00\"}";
        DurableHttpRequest req = mapper.readValue(json, DurableHttpRequest.class);
        assertEquals(Duration.ofMinutes(5), req.getTimeout());
    }

    @Test
    void deserializeWithRetryOptions() throws Exception {
        String json = "{\"method\":\"GET\",\"uri\":\"https://example.com\","
                + "\"asynchronousPatternEnabled\":true,"
                + "\"retryOptions\":{\"firstRetryInterval\":\"00:00:05\",\"maxNumberOfAttempts\":3}}";
        DurableHttpRequest req = mapper.readValue(json, DurableHttpRequest.class);
        assertNotNull(req.getHttpRetryOptions());
        assertEquals(3, req.getHttpRetryOptions().getMaxNumberOfAttempts());
        assertEquals(Duration.ofSeconds(5), req.getHttpRetryOptions().getFirstRetryInterval());
    }

    // ---- JSON round-trip tests ----

    @Test
    void roundTripMinimalRequest() throws Exception {
        DurableHttpRequest original = new DurableHttpRequest("DELETE", URI.create("https://example.com/item/1"));
        String json = mapper.writeValueAsString(original);
        DurableHttpRequest deserialized = mapper.readValue(json, DurableHttpRequest.class);
        assertEquals(original.getMethod(), deserialized.getMethod());
        assertEquals(original.getUri(), deserialized.getUri());
        assertEquals(original.isAsynchronousPatternEnabled(), deserialized.isAsynchronousPatternEnabled());
    }

    @Test
    void roundTripWithTimeoutAndRetry() throws Exception {
        HttpRetryOptions retry = new HttpRetryOptions(Duration.ofSeconds(10), 5);
        retry.setBackoffCoefficient(2.0);
        retry.setStatusCodesToRetry(Arrays.asList(429, 500, 503));
        DurableHttpRequest original = new DurableHttpRequest("GET", URI.create("https://example.com"),
                Collections.singletonMap("Accept", "application/json"), "body", null, true,
                Duration.ofMinutes(15), retry);
        String json = mapper.writeValueAsString(original);
        DurableHttpRequest deserialized = mapper.readValue(json, DurableHttpRequest.class);
        assertEquals(original.getMethod(), deserialized.getMethod());
        assertEquals(original.getUri(), deserialized.getUri());
        assertEquals(original.getHeaders(), deserialized.getHeaders());
        assertEquals(original.getContent(), deserialized.getContent());
        assertEquals(original.isAsynchronousPatternEnabled(), deserialized.isAsynchronousPatternEnabled());
        assertEquals(original.getTimeout(), deserialized.getTimeout());
        assertEquals(original.getHttpRetryOptions().getMaxNumberOfAttempts(),
                deserialized.getHttpRetryOptions().getMaxNumberOfAttempts());
        assertEquals(original.getHttpRetryOptions().getFirstRetryInterval(),
                deserialized.getHttpRetryOptions().getFirstRetryInterval());
        assertEquals(original.getHttpRetryOptions().getBackoffCoefficient(),
                deserialized.getHttpRetryOptions().getBackoffCoefficient(), 0.001);
        assertEquals(original.getHttpRetryOptions().getStatusCodesToRetry(),
                deserialized.getHttpRetryOptions().getStatusCodesToRetry());
    }

    // ---- Managed Identity serialization tests (parity with .NET DurableHttpTests) ----

    /**
     * Parity with .NET test: SerializeManagedIdentityOptions (Part 2).
     * Verifies that a full DurableHttpRequest with ManagedIdentityTokenSource + options
     * serializes all managed identity fields correctly.
     */
    @Test
    void serializeRequestWithManagedIdentityAndOptions() throws Exception {
        Map<String, String> headers = Collections.singletonMap("Accept", "application/json");
        ManagedIdentityOptions options = new ManagedIdentityOptions(
                URI.create("https://dummy.login.microsoftonline.com/"), "tenant_id");
        ManagedIdentityTokenSource tokenSource = new ManagedIdentityTokenSource("dummy url", options);

        DurableHttpRequest req = new DurableHttpRequest("GET", URI.create("https://www.dummy-url.com"),
                headers, null, tokenSource);

        String json = mapper.writeValueAsString(req);

        // Verify top-level fields
        assertTrue(json.contains("\"method\":\"GET\""));
        assertTrue(json.contains("\"uri\":\"https://www.dummy-url.com\""));
        assertTrue(json.contains("\"asynchronousPatternEnabled\":true"));

        // Verify tokenSource with options
        assertTrue(json.contains("\"kind\":\"AzureManagedIdentity\""));
        assertTrue(json.contains("\"resource\":\"dummy url\""));
        assertTrue(json.contains("\"options\""));
        assertTrue(json.contains("\"authorityhost\":\"https://dummy.login.microsoftonline.com/\""));
        assertTrue(json.contains("\"tenantid\":\"tenant_id\""));

        // Verify null fields omitted
        assertFalse(json.contains("\"content\""));
        assertFalse(json.contains("\"timeout\""));
        assertFalse(json.contains("\"retryOptions\""));
    }

    /**
     * Parity with .NET test: SerializeDurableHttpRequestWithoutManagedIdentityOptions.
     * Verifies that a DurableHttpRequest with ManagedIdentityTokenSource but NO options
     * omits the options field in the serialized JSON.
     */
    @Test
    void serializeRequestWithManagedIdentityWithoutOptions() throws Exception {
        Map<String, String> headers = Collections.singletonMap("Accept", "application/json");
        ManagedIdentityTokenSource tokenSource = new ManagedIdentityTokenSource("dummy url");

        DurableHttpRequest req = new DurableHttpRequest("GET", URI.create("https://www.dummy-url.com"),
                headers, null, tokenSource);

        String json = mapper.writeValueAsString(req);

        // Verify tokenSource present
        assertTrue(json.contains("\"kind\":\"AzureManagedIdentity\""));
        assertTrue(json.contains("\"resource\":\"dummy url\""));

        // Verify options field is NOT in the JSON (null → omitted)
        assertFalse(json.contains("\"options\""));
        assertFalse(json.contains("\"authorityhost\""));
        assertFalse(json.contains("\"tenantid\""));
    }

    // ---- Managed Identity deserialization tests ----

    /**
     * Parity with .NET test: DeserializeManagedIdentityOptions (Part 2).
     * Verifies that a serialized DurableHttpRequest with ManagedIdentityTokenSource + options
     * correctly deserializes back with the options fields preserved.
     */
    @Test
    void deserializeRequestWithManagedIdentityAndOptions() throws Exception {
        Map<String, String> headers = Collections.singletonMap("Accept", "application/json");
        ManagedIdentityOptions options = new ManagedIdentityOptions(
                URI.create("https://dummy.login.microsoftonline.com/"), "tenant_id");
        ManagedIdentityTokenSource tokenSource = new ManagedIdentityTokenSource("dummy url", options);

        DurableHttpRequest original = new DurableHttpRequest("GET", URI.create("https://www.dummy-url.com"),
                headers, null, tokenSource);

        String json = mapper.writeValueAsString(original);
        DurableHttpRequest deserialized = mapper.readValue(json, DurableHttpRequest.class);

        // Verify TokenSource is correctly restored
        assertNotNull(deserialized.getTokenSource());
        assertInstanceOf(ManagedIdentityTokenSource.class, deserialized.getTokenSource());

        ManagedIdentityTokenSource deserializedToken =
                (ManagedIdentityTokenSource) deserialized.getTokenSource();
        assertEquals("dummy url", deserializedToken.getResource());
        assertNotNull(deserializedToken.getOptions());
        assertEquals(URI.create("https://dummy.login.microsoftonline.com/"),
                deserializedToken.getOptions().getAuthorityHost());
        assertEquals("tenant_id", deserializedToken.getOptions().getTenantId());
    }

    /**
     * Parity with .NET test: DeserializeWorkerDurableHttpRequestCorrectly.
     * Verifies cross-extension compatibility by deserializing a raw JSON payload
     * (as would be sent from Worker.Extensions.DurableTask) and verifying all fields.
     */
    @Test
    void deserializeWorkerDurableHttpRequestCorrectly() throws Exception {
        // Raw input mimicking Worker.Extensions.DurableTask with options
        String rawInputFromWorkerExtensions =
                "{\"method\":\"GET\","
                + "\"uri\":\"https://httpbin.org/get\","
                + "\"headers\":null,"
                + "\"content\":null,"
                + "\"tokenSource\":{"
                +     "\"kind\":\"AzureManagedIdentity\","
                +     "\"resource\":\"https://graph.microsoft.com/.default\","
                +     "\"options\":{"
                +         "\"authorityhost\":\"https://login.microsoftonline.com/\","
                +         "\"tenantid\":\"test-tenant-id\""
                +     "}"
                + "},"
                + "\"asynchronousPatternEnabled\":false,"
                + "\"retryOptions\":null,"
                + "\"timeout\":null}";

        DurableHttpRequest req = mapper.readValue(rawInputFromWorkerExtensions, DurableHttpRequest.class);

        // Validate top-level fields
        assertNotNull(req);
        assertEquals("GET", req.getMethod());
        assertEquals(URI.create("https://httpbin.org/get"), req.getUri());
        assertNull(req.getContent());
        assertFalse(req.isAsynchronousPatternEnabled());
        assertNull(req.getHttpRetryOptions());
        assertNull(req.getTimeout());

        // Validate the TokenSource was correctly deserialized with options
        assertNotNull(req.getTokenSource());
        assertInstanceOf(ManagedIdentityTokenSource.class, req.getTokenSource());

        ManagedIdentityTokenSource tokenSource = (ManagedIdentityTokenSource) req.getTokenSource();
        assertEquals("https://graph.microsoft.com/.default", tokenSource.getResource());
        assertNotNull(tokenSource.getOptions());
        assertEquals(URI.create("https://login.microsoftonline.com/"),
                tokenSource.getOptions().getAuthorityHost());
        assertEquals("test-tenant-id", tokenSource.getOptions().getTenantId());
    }

    // ---- Managed Identity round-trip tests ----

    /**
     * Full round-trip test: DurableHttpRequest with ManagedIdentityTokenSource + options
     * serialized then deserialized preserves all fields.
     */
    @Test
    void roundTripWithManagedIdentityAndOptions() throws Exception {
        Map<String, String> headers = Collections.singletonMap("Accept", "application/json");
        ManagedIdentityOptions options = new ManagedIdentityOptions(
                URI.create("https://login.microsoftonline.us/"), "us-gov-tenant");
        ManagedIdentityTokenSource tokenSource = new ManagedIdentityTokenSource(
                "https://management.core.windows.net/.default", options);
        HttpRetryOptions retry = new HttpRetryOptions(Duration.ofSeconds(1), 3);

        DurableHttpRequest original = new DurableHttpRequest("POST",
                URI.create("https://api.example.com/resource"),
                headers, "request-body", tokenSource, false, Duration.ofMinutes(5), retry);

        String json = mapper.writeValueAsString(original);
        DurableHttpRequest deserialized = mapper.readValue(json, DurableHttpRequest.class);

        // Verify all top-level fields
        assertEquals(original.getMethod(), deserialized.getMethod());
        assertEquals(original.getUri(), deserialized.getUri());
        assertEquals(original.getHeaders(), deserialized.getHeaders());
        assertEquals(original.getContent(), deserialized.getContent());
        assertEquals(original.isAsynchronousPatternEnabled(), deserialized.isAsynchronousPatternEnabled());
        assertEquals(original.getTimeout(), deserialized.getTimeout());

        // Verify retry
        assertNotNull(deserialized.getHttpRetryOptions());
        assertEquals(original.getHttpRetryOptions().getMaxNumberOfAttempts(),
                deserialized.getHttpRetryOptions().getMaxNumberOfAttempts());

        // Verify managed identity token source
        assertNotNull(deserialized.getTokenSource());
        assertInstanceOf(ManagedIdentityTokenSource.class, deserialized.getTokenSource());
        ManagedIdentityTokenSource deserializedToken =
                (ManagedIdentityTokenSource) deserialized.getTokenSource();
        assertEquals("https://management.core.windows.net/.default", deserializedToken.getResource());
        assertNotNull(deserializedToken.getOptions());
        assertEquals(URI.create("https://login.microsoftonline.us/"),
                deserializedToken.getOptions().getAuthorityHost());
        assertEquals("us-gov-tenant", deserializedToken.getOptions().getTenantId());
    }

    /**
     * Round-trip test: DurableHttpRequest with ManagedIdentityTokenSource but NO options.
     */
    @Test
    void roundTripWithManagedIdentityWithoutOptions() throws Exception {
        ManagedIdentityTokenSource tokenSource = new ManagedIdentityTokenSource("https://vault.azure.net/.default");

        DurableHttpRequest original = new DurableHttpRequest("GET",
                URI.create("https://myvault.vault.azure.net/secrets/mysecret"),
                null, null, tokenSource);

        String json = mapper.writeValueAsString(original);
        DurableHttpRequest deserialized = mapper.readValue(json, DurableHttpRequest.class);

        assertNotNull(deserialized.getTokenSource());
        assertInstanceOf(ManagedIdentityTokenSource.class, deserialized.getTokenSource());
        ManagedIdentityTokenSource deserializedToken =
                (ManagedIdentityTokenSource) deserialized.getTokenSource();
        assertEquals("https://vault.azure.net/.default", deserializedToken.getResource());
        assertNull(deserializedToken.getOptions());
    }
}
