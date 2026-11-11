// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.azurefunctions;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DurableClientContextTest {
    private final Gson gson = new Gson();

    @Test
    void bindingHeadersAreIncludedInPayloadAndLocationIsKeyFree() {
        String binding = "{\"rpcBaseUrl\":\"http://127.0.0.1:12345\","
                + "\"requiredQueryStringParameters\":\"taskHub=hub&connection=storage\","
                + "\"requiredHeaders\":{\"x-functions-key\":\"synthetic-key\"}}";
        assertCheckStatusResponse(binding, "synthetic-key",
                "?taskHub=hub&connection=storage");
    }

    @Test
    void legacyBindingQueryKeyIsConvertedToHeaderMetadata() {
        String binding = "{\"requiredQueryStringParameters\":\"CoDe=synthetic%2Bkey&site=example\"}";
        assertCheckStatusResponse(binding, "synthetic+key", "?site=example");
    }

    @Test
    void bindingCredentialsRemainScopedToEachContext() {
        DurableClientContext first = this.gson.fromJson(
                "{\"requiredHeaders\":{\"x-functions-key\":\"first\"}}", DurableClientContext.class);
        DurableClientContext second = this.gson.fromJson(
                "{\"requiredHeaders\":{\"x-functions-key\":\"second\"}}", DurableClientContext.class);
        HttpRequestMessage<?> request = request();

        assertEquals("first", first.createHttpManagementPayload(request, "instance")
                .getRequiredHeaders().get("x-functions-key"));
        assertEquals("second", second.createHttpManagementPayload(request, "instance")
                .getRequiredHeaders().get("x-functions-key"));
    }

    private void assertCheckStatusResponse(String binding, String key, String query) {
        DurableClientContext context = this.gson.fromJson(binding, DurableClientContext.class);
        HttpRequestMessage<?> request = request();
        HttpResponseMessage.Builder builder = mock(HttpResponseMessage.Builder.class, RETURNS_SELF);
        HttpResponseMessage response = mock(HttpResponseMessage.class);
        when(request.createResponseBuilder(HttpStatus.ACCEPTED)).thenReturn(builder);
        when(builder.build()).thenReturn(response);

        assertSame(response, context.createCheckStatusResponse(request, "instance"));

        ArgumentCaptor<Object> body = ArgumentCaptor.forClass(Object.class);
        verify(builder).body(body.capture());
        HttpManagementPayload payload = (HttpManagementPayload) body.getValue();
        String expectedUrl = "https://example.com/runtime/webhooks/durabletask/instances/instance" + query;
        assertEquals(expectedUrl, payload.getStatusQueryGetUri());
        verify(builder).header("Location", expectedUrl);
        verify(builder).header("Cache-Control", "no-store");
        JsonObject json = this.gson.toJsonTree(payload).getAsJsonObject();
        assertEquals(key, json.getAsJsonObject("requiredHeaders").get("x-functions-key").getAsString());
        assertFalse(payload.getStatusQueryGetUri().contains("code="));
        assertFalse(payload.getStatusQueryGetUri().contains("synthetic"));
    }

    private static HttpRequestMessage<?> request() {
        HttpRequestMessage<?> request = mock(HttpRequestMessage.class);
        when(request.getUri()).thenReturn(URI.create("https://example.com/api/start?code=starter-key"));
        return request;
    }
}
